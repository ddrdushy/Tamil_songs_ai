import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
import hashlib


BASE_URL = "https://www.tamil2lyrics.com"
MOVIE_LIST_URL = f"{BASE_URL}/movie/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TamilLyricsScraper/1.0; +https://github.com/your-repo)"
}

def compute_hash(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

def get_soup(url, retries=3, delay=3):
    """
    GET a URL and return BeautifulSoup object, with retry on network errors.
    Raises the last exception if all retries fail.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            print(f"[ERROR] Request failed ({attempt}/{retries}) for {url}: {e}")
            if attempt == retries:
                # Give up after last retry
                raise
            time.sleep(delay)


def has_tamil(text: str) -> bool:
    """Return True if any character is in the Tamil Unicode block."""
    return any('\u0b80' <= ch <= '\u0bff' for ch in text)


def parse_movie_list_page(page: int):
    """
    Return list of movie URLs from a single movie list page.
    Also returns whether a 'Next' page exists.
    """
    if page == 1:
        url = MOVIE_LIST_URL
    else:
        url = f"{MOVIE_LIST_URL}page/{page}/"

    print(f"[INFO] Movie list page {page}: {url}")
    soup = get_soup(url)

    movie_urls = set()

    # Strategy: all <a> whose href contains '/movies/'
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/movies/" in href:
            if href.startswith("/"):
                href = BASE_URL + href
            movie_urls.add(href)

    # Rough 'Next' existence check
    has_next = any("Next" in (a.get_text(strip=True) or "") for a in soup.find_all("a"))

    return sorted(movie_urls), has_next


def parse_movie_page(movie_url):
    """
    From a movie URL, get:
      - Movie title
      - Movie year
      - List of (song_title, song_url)
    """
    print(f"[MOVIE] {movie_url}")
    soup = get_soup(movie_url)

    # Movie title + year (e.g. "10 Enradhukulla(2015)")
    movie_title = ""
    movie_year = ""
    h_tag = soup.find(["h1", "h2", "h3"])
    if h_tag:
        title_text = h_tag.get_text(strip=True)
        m = re.match(r"(.+)\((\d{4})\)", title_text)
        if m:
            movie_title = m.group(1).strip()
            movie_year = m.group(2)
        else:
            movie_title = title_text

    song_links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Only lyrics pages
        if "/lyrics/" not in href:
            continue

        # Skip campaign URLs with UTM tracking
        if "utm_source=" in href or "utm_medium=" in href or "utm_campaign=" in href:
            continue

        # Normalize: strip querystring completely
        if "?" in href:
            href = href.split("?", 1)[0]

        if href.startswith("/"):
            href = BASE_URL + href

        title = a.get_text(strip=True)
        key = (title, href)
        if key not in seen:
            seen.add(key)
            song_links.append((title, href))

    return movie_title, movie_year, song_links


def parse_song_page(song_url):
    """
    From a song URL, extract:
      - Singer
      - Music by
      - English lyrics (romanized)
      - Tamil lyrics (Unicode)
    """
    print(f"    [SONG] {song_url}")
    soup = get_soup(song_url)

    full_text = soup.get_text("\n")
    full_text = re.sub(r"\r", "", full_text)

    # --- META: Singer ---
    singer = ""
    m_singer = re.search(r"Singer\s*:\s*(.+)", full_text)
    if m_singer:
        singer = m_singer.group(1).strip()

    # Try Tamil label if English not found
    if not singer:
        m_singer_ta = re.search(r"பாடகர்\s*:\s*(.+)", full_text)
        if m_singer_ta:
            singer = m_singer_ta.group(1).strip()

    # --- META: Music by ---
    music_by = ""
    m_music = re.search(r"Music by\s*:\s*(.+)", full_text)
    if m_music:
        music_by = m_music.group(1).strip()

    if not music_by:
        m_music_ta = re.search(r"இசையமைப்பாளர்\s*:\s*(.+)", full_text)
        if m_music_ta:
            music_by = m_music_ta.group(1).strip()

    # --- LYRICS SECTION ---
    # Prefer starting after "English தமிழ்" if present
    lyrics_text = ""
    marker = "English தமிழ்"
    idx = full_text.find(marker)
    if idx != -1:
        lyrics_text = full_text[idx + len(marker):]
    else:
        if m_music:
            lyrics_text = full_text[m_music.end():]
        else:
            lyrics_text = full_text

    # Cut off at common footer markers (including "Other Songs from")
    stop_markers = [
        "Other Songs from",
        "Added by",
        "© 2025 - www.tamil2lyrics.com",
        "© 2024 - www.tamil2lyrics.com",
        "© 2023 - www.tamil2lyrics.com",
        "© 2022 - www.tamil2lyrics.com",
        "© 2021 - www.tamil2lyrics.com",
    ]
    stop_idx = len(lyrics_text)
    for mk in stop_markers:
        pos = lyrics_text.find(mk)
        if pos != -1:
            stop_idx = min(stop_idx, pos)
    lyrics_text = lyrics_text[:stop_idx].strip()

    # Split into lines and clean
    lines = [ln.strip() for ln in lyrics_text.split("\n")]

    # Trim leading/trailing empty lines
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    english_lines = []
    tamil_lines = []

    for line in lines:
        if not line:
            # preserve stanza gaps
            english_lines.append("")
            tamil_lines.append("")
            continue

        # Skip meta labels we already used
        if ("Singer :" in line or
            "Music by" in line or
            "English தமிழ்" in line or
            "பாடகர்" in line or
            "இசையமைப்பாளர்" in line):
            continue

        if has_tamil(line):
            tamil_lines.append(line)
        else:
            english_lines.append(line)

    def normalize_block(block_lines):
        cleaned = []
        prev_empty = False
        for l in block_lines:
            is_empty = (l.strip() == "")
            if is_empty and prev_empty:
                continue
            cleaned.append(l)
            prev_empty = is_empty
        # strip outer empties
        while cleaned and cleaned[0].strip() == "":
            cleaned.pop(0)
        while cleaned and cleaned[-1].strip() == "":
            cleaned.pop()
        return "\n".join(cleaned)

    english_lyrics = normalize_block(english_lines)
    tamil_lyrics = normalize_block(tamil_lines)

    return singer, music_by, english_lyrics, tamil_lyrics


def load_scraped_urls(output_file):
    """
    Read an existing JSONL file and return a set of already-scraped song URLs.
    This lets us resume without duplicating work.
    """
    scraped = set()
    if not os.path.exists(output_file):
        return scraped

    print(f"[INFO] Loading already scraped songs from {output_file} ...")
    with open(output_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = rec.get("song_url")
            if url:
                scraped.add(url)

    print(f"[INFO] Found {len(scraped)} songs already scraped.")
    return scraped

def load_existing_index(output_file):
    """
    Returns dict: song_url -> last_seen_source_hash
    """
    existing = {}
    if not os.path.exists(output_file):
        return existing

    print(f"[INFO] Loading existing songs from {output_file} ...")
    with open(output_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            url = rec.get("song_url")
            h = rec.get("source_hash")
            if url and h:
                # last one wins (append-only history)
                existing[url] = h

    print(f"[INFO] Loaded {len(existing)} unique song URLs.")
    return existing


def scrape_all_json(output_file="tamil2lyrics_songs.jsonl", max_pages=None):
    """
    Crawl all movie pages, all songs, and store into a JSON Lines file.
    Supports resume: if the file exists, load existing song URLs and skip them.
    """
    existing_index = load_existing_index(output_file)
    file_mode = "a" if os.path.exists(output_file) else "w"

    with open(output_file, file_mode, encoding="utf-8") as f:
        page = 1
        while True:
            if max_pages and page > max_pages:
                break

            try:
                movie_urls, has_next = parse_movie_list_page(page)
            except requests.RequestException as e:
                print(f"[ERROR] Movie list page {page} failed: {e}")
                # skip this page and try next
                if not has_next:
                    break
                page += 1
                continue

            if not movie_urls:
                print("[INFO] No more movie URLs found, stopping.")
                break

            for movie_url in movie_urls:
                time.sleep(1)  # be polite
                try:
                    movie_title, movie_year, songs = parse_movie_page(movie_url)
                except requests.RequestException as e:
                    print(f"[ERROR] Failed to parse movie {movie_url}: {e}")
                    continue

                for song_title, song_url in songs:
                    # Resume support: skip if we already have this song
                    previous_hash = existing_index.get(song_url)


                    time.sleep(1)
                    try:
                        singer, music_by, english_lyrics, tamil_lyrics = parse_song_page(song_url)
                    except requests.RequestException as e:
                        print(f"      [ERROR] Failed song {song_url}: {e}")
                        # don't crash, just skip this song
                        continue
                    except Exception as e:
                        print(f"      [ERROR] Unexpected error on {song_url}: {e}")
                        continue

                    source_text = (tamil_lyrics or "") + "\n" + (english_lyrics or "")
                    current_hash = compute_hash(source_text)
                    
                    if previous_hash == current_hash:
                        print(f"    [SKIP] Unchanged {song_url}")
                        continue

                    status = "NEW" if previous_hash is None else "UPDATED"
                    print(f"    [{status}] {song_url}")

                    record = {
                        "movie_title": movie_title,
                        "movie_year": movie_year,
                        "movie_url": movie_url,
                        "song_title": song_title,
                        "song_url": song_url,
                        "singer": singer,
                        "music_by": music_by,
                        "english_lyrics": english_lyrics,
                        "tamil_lyrics": tamil_lyrics,
                        "source_hash": current_hash,
                    }

                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    f.flush()

                    # update index so repeated songs in same run don't reprocess
                    existing_index[song_url] = current_hash
                    

            if not has_next:
                print("[INFO] No Next page, finished all movies.")
                break

            page += 1


if __name__ == "__main__":
    # For a small test:
    # scrape_all_json(max_pages=1)

    scrape_all_json()
    print("Done. Data saved to tamil2lyrics_songs.jsonl")
