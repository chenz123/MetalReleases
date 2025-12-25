import json
import time
import re
import cloudscraper
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURATION ---
GENRE_IDS = [
   "62-black-metal",
    "556-blackened-death-metal",
    "89-death-metal",
    "90-melodic-death-metal",
    "658-progressive-death-metal",
    "51-doom-metal",
    "105-gothic-metal",
    "135-groove-metal",
    "59-metalcore",
    "54-progressive-metal",
    "120-power-metal",
    "88-thrash-metal",
    "279-symphonic-metal",
    "40-metal",
    "687-neoclassical-metal",
    "107-industrial-metal",
    "291-djent"
]

def scrape_releases():
    # 1. Setup Scraper for AOTY (Needs Cloudflare bypass)
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    # 2. Setup Date Range
    today = datetime.now()
    current_year = today.year
    start_limit = today - timedelta(days=60)
    end_limit = today + timedelta(days=7)
    
    # MusicBrainz uses strict ISO dates for search queries
    mb_start = start_limit.strftime('%Y-%m-%d')
    mb_end = end_limit.strftime('%Y-%m-%d')
    
    print(f"SYSTEM DATE: {today.date()}")
    print(f"SEARCH RANGE: {start_limit.date()} to {end_limit.date()}\n")

    # Shared Dictionary for Deduplication: Key = (Title, Artist)
    albums_map = {}

    # =========================================================================
    # PART 1: ALBUM OF THE YEAR (AOTY) SCRAPING
    # =========================================================================
    print("=== STARTING AOTY SCRAPE ===")
    
    for genre_id in GENRE_IDS:
        clean_genre = " ".join(genre_id.split('-')[1:]).title()
        print(f"\n>>> PROCESSING AOTY: {clean_genre} <<<")

        sources = [
            ("Upcoming", f"https://www.albumoftheyear.org/upcoming/genre/{genre_id}/"),
            ("Recent", f"https://www.albumoftheyear.org/genre/{genre_id}/recent/")
        ]

        for source_name, base_url in sources:
            page = 1
            keep_scraping = True
            
            while keep_scraping:
                if source_name == "Recent":
                    url = f"{base_url}?page={page}"
                else:
                    if page > 1: break 
                    url = base_url

                print(f"   [{source_name}] Page {page}...", end="\r")
                
                try:
                    response = scraper.get(url)
                    if response.status_code != 200: break

                    soup = BeautifulSoup(response.text, 'html.parser')
                    blocks = soup.select('.albumBlock, .albumListRow')
                    
                    if not blocks: break
                    
                    for block in blocks:
                        # Data Extraction
                        title_node = block.select_one('.albumTitle, .title, .albumListTitle')
                        if not title_node: continue
                        title = title_node.text.strip()
                        
                        artist_node = block.select_one('.artistTitle, .artist, .albumListArtist')
                        artist = artist_node.text.strip() if artist_node else "Unknown"

                        unique_key = (title.lower(), artist.lower())

                        # Deduplication Logic
                        if unique_key in albums_map:
                            if clean_genre not in albums_map[unique_key]['genre_list']:
                                albums_map[unique_key]['genre_list'].append(clean_genre)
                                albums_map[unique_key]['genre'] = ", ".join(albums_map[unique_key]['genre_list'])
                            continue

                        # Date Parsing
                        block_text = block.get_text(" ", strip=True)
                        match = re.search(r'([A-Z][a-z]{2,8})\.?\s(\d{1,2})(?:,\s(\d{4}))?', block_text)
                        if not match: continue

                        month_str, day_str, year_str = match.groups()
                        
                        # Year Logic
                        if year_str:
                            album_year = int(year_str)
                        else:
                            album_year = current_year
                            if today.month == 1 and month_str in ["Dec", "December"]: album_year -= 1
                            elif today.month == 12 and month_str in ["Jan", "January"]: album_year += 1

                        clean_month = month_str.replace("Sept", "September").replace("Jan", "January").replace("Feb", "February").replace("Aug", "August").replace("Oct", "October").replace("Dec", "December")
                        date_str_full = f"{clean_month} {day_str}, {album_year}"
                        
                        try:
                            rel_date = datetime.strptime(date_str_full, '%B %d, %Y')
                        except ValueError:
                            try:
                                rel_date = datetime.strptime(date_str_full, '%b %d, %Y')
                            except ValueError: continue

                        # Filtering
                        if source_name == "Recent" and rel_date < start_limit:
                            keep_scraping = False; break
                        
                        if start_limit <= rel_date <= end_limit:
                            link_tag = block.find('a')
                            link = f"https://www.albumoftheyear.org{link_tag['href']}" if link_tag else ""
                            
                            albums_map[unique_key] = {
                                "artist": artist,
                                "album": title,
                                "release_date": rel_date.strftime('%Y-%m-%d'),
                                "genre": clean_genre,
                                "genre_list": [clean_genre],
                                "url": link,
                                "source": "AOTY"
                            }

                    if not keep_scraping: break
                    page += 1
                    if page > 2: break 
                    time.sleep(1)

                except Exception: break
        print(f"   Done.                               ")

   # =========================================================================
    # PART 2: MUSICBRAINZ (MB) SCRAPING (FIXED)
    # =========================================================================
    print("\n=== STARTING MUSICBRAINZ SCRAPE ===")
    
    # MusicBrainz is strict. We use a retry adapter and the scraper instance.
    mb_url = "https://musicbrainz.org/ws/2/release"
    
    # We update the headers on the scraper specifically for MB requests
    # Use a real-looking email or keep it formatted correctly
    mb_headers = {
        'User-Agent': 'MetalReleasesApp/1.0 ( action@github.com )',
        'Accept': 'application/json'
    }

    for genre_id in GENRE_IDS:
        mb_tag = " ".join(genre_id.split('-')[1:])
        print(f">>> Querying MB Tag: '{mb_tag}'")
        
        query = (
            f'tag:"{mb_tag}" AND status:official AND (primarytype:Album OR primarytype:EP) '
            f'AND date:[{mb_start} TO {mb_end}]'
        )
        
        params = {
            'query': query,
            'fmt': 'json',
            'limit': 50
        }

        # RETRY LOGIC: Try 3 times before giving up on a genre
        for attempt in range(3):
            try:
                # CHANGED: Use 'scraper' instead of 'requests'
                # This uses browser-like SSL ciphers to prevent ConnectionReset
                resp = scraper.get(mb_url, headers=mb_headers, params=params, timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    releases = data.get('releases', [])
                    print(f"   Found {len(releases)} results...")

                    for rel in releases:
                        title = rel.get('title', '').strip()
                        artist_credit = rel.get('artist-credit', [])
                        artist = artist_credit[0]['name'].strip() if artist_credit else "Unknown"
                        date_str = rel.get('date', '')

                        if len(date_str) == 10:
                            try:
                                rel_date = datetime.strptime(date_str, '%Y-%m-%d')
                            except ValueError: continue
                        else:
                            continue

                        if start_limit <= rel_date <= end_limit:
                            unique_key = (title.lower(), artist.lower())
                            clean_genre_title = mb_tag.title()

                            if unique_key in albums_map:
                                if clean_genre_title not in albums_map[unique_key]['genre_list']:
                                    albums_map[unique_key]['genre_list'].append(clean_genre_title)
                                    albums_map[unique_key]['genre'] = ", ".join(albums_map[unique_key]['genre_list'])
                            else:
                                mb_link = f"https://musicbrainz.org/release/{rel['id']}"
                                albums_map[unique_key] = {
                                    "artist": artist,
                                    "album": title,
                                    "release_date": rel_date.strftime('%Y-%m-%d'),
                                    "genre": clean_genre_title,
                                    "genre_list": [clean_genre_title],
                                    "url": mb_link,
                                    "source": "MusicBrainz"
                                }
                    # Break the retry loop if successful
                    break 
                
                elif resp.status_code == 503:
                    # 503 means "Slow down", so we wait longer and retry
                    print("   [503] Rate limit hit. Waiting 5s...")
                    time.sleep(5)
                    continue
                else:
                    print(f"   [Error] MB Status {resp.status_code}")
                    break

            except Exception as e:
                print(f"   [Attempt {attempt+1}] Connection error: {e}")
                time.sleep(2) # Wait a bit before retry

        # RATE LIMITING: Valid requests still need a pause
        time.sleep(1.5)
    # =========================================================================
    # SAVE OUTPUT
    # =========================================================================
    final_output = []
    for item in albums_map.values():
        item_copy = item.copy()
        del item_copy['genre_list'] 
        final_output.append(item_copy)
    
    final_output.sort(key=lambda x: x['release_date'], reverse=True)

    with open('metal_releases.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)
    
    print(f"\nSUCCESS: Saved {len(final_output)} unique albums.")

if __name__ == "__main__":
    scrape_releases()