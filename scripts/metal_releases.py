import json
import time
import re
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

def scrape_metal_releases():
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    # --- CONFIGURATION ---
    # Specific genres first, generic "Metal" last
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
        "40-metal"
    ]
    
    today = datetime.now()
    current_year = today.year
    start_limit = today - timedelta(days=60)
    end_limit = today + timedelta(days=7)
    
    print(f"SYSTEM DATE: {today.date()}")
    print(f"SEARCH RANGE: {start_limit.date()} to {end_limit.date()}\n")

    # Dictionary to handle duplicates: Key = (Title, Artist)
    # This allows us to find an existing album and just append the new genre
    albums_map = {}

    # --- MAIN LOOP ---
    for genre_id in GENRE_IDS:
        # Format Genre Name: "556-blackened-death-metal" -> "Blackened Death Metal"
        clean_genre = " ".join(genre_id.split('-')[1:]).title()
        print(f"\n>>> PROCESSING: {clean_genre} <<<")

        sources = [
            ("Upcoming", f"https://www.albumoftheyear.org/upcoming/genre/{genre_id}/"),
            ("Recent", f"https://www.albumoftheyear.org/genre/{genre_id}/recent/")
        ]

        for source_name, base_url in sources:
            page = 1
            keep_scraping = True
            
            while keep_scraping:
                # Pagination setup
                if source_name == "Recent":
                    url = f"{base_url}?page={page}"
                else:
                    if page > 1: break 
                    url = base_url

                print(f"   [{source_name}] Page {page}...", end="\r") # Overwrite line for cleaner logs
                
                try:
                    response = scraper.get(url)
                    if response.status_code != 200:
                        break # Skip source if error

                    soup = BeautifulSoup(response.text, 'html.parser')
                    blocks = soup.select('.albumBlock, .albumListRow')
                    
                    if not blocks:
                        break

                    valid_count = 0
                    
                    for block in blocks:
                        # 1. Info Extraction
                        title_node = block.select_one('.albumTitle, .title, .albumListTitle')
                        if not title_node: continue
                        title = title_node.text.strip()
                        
                        artist_node = block.select_one('.artistTitle, .artist, .albumListArtist')
                        artist = artist_node.text.strip() if artist_node else "Unknown"

                        # 2. Key for uniqueness
                        unique_key = (title.lower(), artist.lower())

                        # 3. IF ALBUM ALREADY EXISTS: Just update Genre
                        if unique_key in albums_map:
                            existing_genres = albums_map[unique_key]['genre_list']
                            if clean_genre not in existing_genres:
                                existing_genres.append(clean_genre)
                                # Update string representation
                                albums_map[unique_key]['genre'] = ", ".join(existing_genres)
                            continue # Move to next block, don't re-parse date

                        # 4. IF NEW: Parse Date and Add
                        block_text = block.get_text(" ", strip=True)
                        match = re.search(r'([A-Z][a-z]{2,8})\.?\s(\d{1,2})(?:,\s(\d{4}))?', block_text)
                        
                        if not match: continue

                        month_str, day_str, year_str = match.groups()

                        if year_str:
                            album_year = int(year_str)
                        else:
                            album_year = current_year
                            if today.month == 1 and month_str in ["Dec", "December"]:
                                album_year = current_year - 1
                            elif today.month == 12 and month_str in ["Jan", "January"]:
                                album_year = current_year + 1

                        clean_month = month_str.replace("Sept", "September").replace("Jan", "January").replace("Feb", "February").replace("Aug", "August").replace("Oct", "October").replace("Dec", "December")
                        date_str_full = f"{clean_month} {day_str}, {album_year}"
                        
                        try:
                            rel_date = datetime.strptime(date_str_full, '%B %d, %Y')
                        except ValueError:
                            try:
                                rel_date = datetime.strptime(date_str_full, '%b %d, %Y')
                            except ValueError:
                                continue

                        # 5. Logic Checks
                        if source_name == "Recent" and rel_date < start_limit:
                            keep_scraping = False # Stop this genre source
                            break

                        if start_limit <= rel_date <= end_limit:
                            link_tag = block.find('a')
                            link = f"https://www.albumoftheyear.org{link_tag['href']}" if link_tag else ""
                            
                            # Create new entry
                            albums_map[unique_key] = {
                                "artist": artist,
                                "album": title,
                                "release_date": rel_date.strftime('%Y-%m-%d'),
                                "genre": clean_genre,        # String for JSON
                                "genre_list": [clean_genre], # List for logic
                                "url": link
                            }
                            valid_count += 1

                    # Pagination Logic
                    if not keep_scraping: break
                    
                    page += 1
                    # LIMIT: Stop after Page 2
                    if page > 2: 
                        break 
                    
                    time.sleep(1)

                except Exception:
                    break
        
        # Newline after genre finishes
        print(f"   Done.                               ") 
        time.sleep(1)

    # --- FORMAT OUTPUT ---
    # Convert map back to list and remove the helper 'genre_list' field
    final_output = []
    for item in albums_map.values():
        item_copy = item.copy()
        del item_copy['genre_list'] # Remove helper field before saving
        final_output.append(item_copy)
    
    # Sort by Date (Newest first)
    final_output.sort(key=lambda x: x['release_date'], reverse=True)

    with open('metal_releases.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)
    
    print(f"\nSUCCESS: Saved {len(final_output)} unique albums.")

if __name__ == "__main__":
    scrape_metal_releases()