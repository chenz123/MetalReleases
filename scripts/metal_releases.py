#!/usr/bin/env python3
"""
aggregate_releases.py

- Fetches Metal-Archives upcoming releases via their JSON AJAX endpoint (date range: two months ago .. tomorrow)
- Fetches MetalStorm new releases (first 4 pages)
- Enriches with MusicBrainz (best-effort)
- Respects robots.txt crawl-delay and disallowed paths
- Detects Cloudflare / challenge pages and logs details
- Writes a stateless snapshot to ../snapshot.json
- Logs to console and scripts/aggregator.log
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import musicbrainzngs
import urllib.robotparser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Output file (snapshot)
OUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'snapshot.json')
LOG_FILE = os.path.join(os.path.dirname(__file__), 'aggregator.log')

# Default user agent (browser-like). You can override with METAL_AGG_USER_AGENT env var.
DEFAULT_USER_AGENT = os.environ.get(
    "METAL_AGG_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Secondary user agent (slightly different) used as a fallback attempt
FALLBACK_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15"
)

# Use the chosen UA
USER_AGENT = DEFAULT_USER_AGENT

# Configure MusicBrainz
musicbrainzngs.set_useragent("my-metal-aggregator", "1.0", "https://example.org/contact")

# Logging setup
logger = logging.getLogger("aggregator")
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S%z")

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(fmt)
logger.addHandler(ch)

# File handler
fh = logging.FileHandler(LOG_FILE, encoding="utf8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
logger.addHandler(fh)


# ---------------------------
# Robots.txt helpers
# ---------------------------
def fetch_robots(base_url):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(urljoin(base_url, "/robots.txt"))
    try:
        rp.read()
        logger.debug("Fetched robots.txt for %s", base_url)
    except Exception as e:
        logger.warning("Could not read robots.txt for %s: %s", base_url, e)
        return None
    return rp


def allowed_by_robots(rp, path):
    if not rp:
        return True
    try:
        allowed = rp.can_fetch(USER_AGENT, path)
        logger.debug("robots.txt can_fetch(%s) => %s", path, allowed)
        return allowed
    except Exception:
        return True


def get_crawl_delay(rp):
    if not rp:
        return 3
    try:
        delay = rp.crawl_delay(USER_AGENT)
        if delay is None:
            return 3
        return max(3, int(delay))
    except Exception:
        return 3


# ---------------------------
# Cloudflare detection & safe GET
# ---------------------------
def detect_cloudflare(text, status):
    if status in (403, 429):
        return True
    t = (text or "").lower()
    return any(x in t for x in ("please enable javascript", "cf-chl-bypass", "captcha", "attention required"))


class CloudflareDetected(RuntimeError):
    pass


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8),
       retry=retry_if_exception_type((requests.RequestException, CloudflareDetected)))
def safe_get(url, params=None, headers=None, timeout=20):
    """
    Perform GET with retries. If Cloudflare challenge is detected, raise CloudflareDetected.
    """
    hdrs = headers or {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    }
    logger.debug("GET %s params=%s headers(User-Agent)=%s", url, params, hdrs.get("User-Agent"))
    r = requests.get(url, headers=hdrs, params=params, timeout=timeout)
    logger.debug("Response status: %s", r.status_code)

    if detect_cloudflare(r.text, r.status_code):
        snippet = (r.text or "")[:1000].replace("\n", " ")
        logger.warning("Cloudflare-like response detected (status=%s). Snippet: %s", r.status_code, snippet)
        raise CloudflareDetected(f"Cloudflare challenge detected (status {r.status_code})")

    r.raise_for_status()
    return r


# ---------------------------
# Metal Archives via JSON endpoint
# ---------------------------
def parse_metal_archives_upcoming():
    base = "https://www.metal-archives.com"
    path = "/release/ajax-upcoming/json/1"

    rp = fetch_robots(base)
    if not allowed_by_robots(rp, path):
        logger.info("Metal-Archives robots.txt disallows %s; skipping", path)
        return []

    delay = get_crawl_delay(rp)
    logger.info("Metal-Archives crawl-delay: %ss", delay)
    time.sleep(delay)

    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)
    two_months_ago = today - timedelta(days=60)

    params = {
        "iDisplayStart": 0,
        "iDisplayLength": 200,
        "includeVersions": 0,
        "fromDate": two_months_ago.isoformat(),
        "toDate": tomorrow.isoformat(),
        "sEcho": 1
    }

    url = base + path

    # Try primary UA, then fallback UA if Cloudflare detected
    try:
        r = safe_get(url, params=params, headers={"User-Agent": USER_AGENT})
    except CloudflareDetected:
        logger.info("Primary UA triggered Cloudflare on Metal-Archives; retrying with fallback UA")
        try:
            r = safe_get(url, params=params, headers={"User-Agent": FALLBACK_USER_AGENT})
        except Exception as e:
            logger.error("Metal-Archives blocked after fallback attempt: %s", e)
            return []
    except Exception as e:
        logger.error("Error fetching Metal-Archives: %s", e)
        return []

    try:
        data = r.json()
    except Exception as e:
        logger.error("Invalid JSON from Metal-Archives: %s", e)
        snippet = (r.text or "")[:1000].replace("\n", " ")
        logger.debug("Response snippet: %s", snippet)
        return []

    rows = data.get("aaData", [])
    logger.info("Metal-Archives returned %d rows", len(rows))
    results = []

    for row in rows:
        if len(row) < 3:
            continue
        date_text = row[0]
        artist_html = row[1]
        album_html = row[2]

        artist = BeautifulSoup(artist_html, "html.parser").get_text(strip=True)
        album_soup = BeautifulSoup(album_html, "html.parser")
        album = album_soup.get_text(strip=True)
        link_el = album_soup.find("a")
        link = link_el["href"] if link_el else None

        iso = None
        try:
            iso = datetime.fromisoformat(date_text).date().isoformat()
        except Exception:
            try:
                iso = datetime.strptime(date_text, "%d %b %Y").date().isoformat()
            except Exception:
                logger.debug("Could not parse date '%s' for %s - %s", date_text, artist, album)

        results.append({
            "source": "metal-archives",
            "id": f"ma:{link or artist + '|' + album + '|' + date_text}",
            "artist": artist,
            "title": album,
            "date": iso,
            "url": link
        })

    return results


# ---------------------------
# MetalStorm scraper (first N pages)
# ---------------------------
def parse_metalstorm_pages(pages=4):
    base = "https://metalstorm.net"
    path = "/events/new_releases.php"

    rp = fetch_robots(base)
    if not allowed_by_robots(rp, path):
        logger.info("MetalStorm robots.txt disallows %s; skipping", path)
        return []

    delay = get_crawl_delay(rp)
    logger.info("MetalStorm crawl-delay: %ss", delay)
    results = []

    for p in range(1, pages + 1):
        time.sleep(delay)
        url = f"{base}{path}?page={p}"
        try:
            r = safe_get(url, headers={"User-Agent": USER_AGENT})
        except CloudflareDetected:
            logger.info("Primary UA triggered Cloudflare on MetalStorm page %d; retrying with fallback UA", p)
            try:
                r = safe_get(url, headers={"User-Agent": FALLBACK_USER_AGENT})
            except Exception as e:
                logger.error("MetalStorm page %d blocked after fallback: %s", p, e)
                break
        except Exception as e:
            logger.error("Error fetching MetalStorm page %d: %s", p, e)
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Try a few selectors; site markup may vary
        found = 0
        for item in soup.select(".release") or []:
            title_el = item.select_one(".release-title a")
            artist_el = item.select_one(".release-artist")
            date_el = item.select_one(".release-date")

            title = title_el.get_text(strip=True) if title_el else None
            artist = artist_el.get_text(strip=True) if artist_el else None
            date_text = date_el.get_text(strip=True) if date_el else None

            results.append({
                "source": "metalstorm",
                "id": f"ms:{title_el['href'] if title_el and title_el.get('href') else (artist or '') + '|' + (title or '')}",
                "artist": artist,
                "title": title,
                "date": date_text,
                "url": urljoin(base, title_el["href"]) if title_el and title_el.get("href") else None
            })
            found += 1

        # fallback: try table rows if .release not found
        if found == 0:
            for tr in soup.select("table tr"):
                tds = tr.find_all("td")
                if len(tds) >= 3:
                    artist = tds[0].get_text(strip=True)
                    title = tds[1].get_text(strip=True)
                    date_text = tds[2].get_text(strip=True)
                    results.append({
                        "source": "metalstorm",
                        "id": f"ms:{artist + '|' + title + '|' + date_text}",
                        "artist": artist,
                        "title": title,
                        "date": date_text,
                        "url": None
                    })

    logger.info("MetalStorm returned %d items", len(results))
    return results


# ---------------------------
# MusicBrainz enrichment (best-effort)
# ---------------------------
def enrich_musicbrainz(item):
    if not item.get("artist") or not item.get("title"):
        return None
    try:
        res = musicbrainzngs.search_releases(artist=item["artist"], release=item["title"], limit=1)
        if res.get("release-list"):
            r = res["release-list"][0]
            return {"mbid": r.get("id"), "date": r.get("date")}
    except Exception as e:
        logger.debug("MusicBrainz lookup failed for %s - %s: %s", item.get("artist"), item.get("title"), e)
    return None


# ---------------------------
# Write snapshot
# ---------------------------
def write_output(releases):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(releases),
        "releases": releases
    }
    with open(OUT_FILE, "w", encoding="utf8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %d releases to %s", len(releases), OUT_FILE)


# ---------------------------
# Main
# ---------------------------
def main():
    logger.info("Aggregator run started")

    # Allow overriding UA via env var at runtime
    global USER_AGENT
    USER_AGENT = os.environ.get("METAL_AGG_USER_AGENT", USER_AGENT)
    logger.debug("Using User-Agent: %s", USER_AGENT)

    ma = parse_metal_archives_upcoming()
    ms = parse_metalstorm_pages(4)

    combined = (ma or []) + (ms or [])
    logger.info("Combined items before enrichment: %d", len(combined))

    # Enrich sequentially with small delay to be polite
    for it in combined:
        time.sleep(1)
        mb = enrich_musicbrainz(it)
        if mb:
            it["enriched"] = mb

    write_output(combined)
    logger.info("Aggregator run finished")


if __name__ == "__main__":
    main()
