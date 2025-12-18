import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from sentence_transformers import SentenceTransformer, util

# ========= CONFIG =========

INPUT_FILE = "tamil2lyrics_songs.jsonl"      # from your scraper
OUTPUT_FILE = "tamil2lyrics_songs_enriched.jsonl" # enriched output

# YouTube (optional – leave empty to skip)
YOUTUBE_API_KEY = None ##''    # or hardcode if you want
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"

# Concurrency
MAX_WORKERS = 500        # tune based on CPU / YouTube quota
LOG_EVERY = 10         # print progress every N records

# ========= TAMIL / TANGLISH KEYWORDS FOR HEURISTICS =========

ROMANTIC_WORDS = [
    "kadhal", "kaadhal", "kalyaanam", "love", "romance",
    "nenjam", "manasu", "ullam", "kaadhalan", "kaadhalai",
    "thangam", "thendral", "uyir", "uyire", "kanne", "kanmani",
    "chellam", "kannae", "penne", "nenjame"
]

SAD_WORDS = [
    "azhugiren", "azhuthen", "azhuven", "kanneer", "kanneer",
    "sogam", "thunbam", "pirivu", "poren", "porattam",
    "thuyaram", "veruthu", "udaindhu", "kavala", "irul"
]

KUTHU_WORDS = [
    "kuthu", "kuththu", "gaana", "gana", "mass", "party",
    "dance", "aadunga", "aadu", "aadi", "sarakku", "local", "beat"
]

DEVOTIONAL_WORDS = [
    "deivam", "devan", "kadavul", "amman", "muruga", "murugan",
    "ayyappa", "krishna", "kannan", "shiva", "siva", "ganesha",
    "vinayaga", "perumal", "ramar", "govinda", "ammane", "swami"
]

FRIENDSHIP_WORDS = [
    "nanban", "nanba", "nanbargal", "friend", "friends", "friendship",
    "thozhan", "thozhi", "machan", "machi", "da machi"
]

ANGRY_WORDS = [
    "kobam", "kovam", "sattam", "porattam",
    "sandai", "sanda", "uttura", "katti", "kathi"
]

# ========= HF MODEL (EMBEDDINGS) =========

print("[INFO] Loading sentence-transformer model...")
MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# Label descriptions
MOOD_LABEL_TEXTS = {
    "romantic":      "A romantic Tamil love song full of feelings and affection",
    "sad":           "A very sad emotional Tamil song full of pain and tears",
    "kuthu":         "A fast energetic kuthu dance song with party vibe",
    "devotional":    "A spiritual devotional Tamil song about god and prayer",
    "happy":         "A happy joyful Tamil song with celebration and smiles",
    "melancholic":   "A slow melancholic Tamil song with deep longing",
    "angry":         "An angry intense Tamil song full of rage and fight",
    "inspirational": "An inspirational motivational Tamil song about hope and success",
    "unknown":       "A neutral Tamil song with general feelings and story"
}

THEME_LABEL_TEXTS = {
    "love":         "The theme is romantic love between two people",
    "heartbreak":   "The theme is breakup and heart break and separation",
    "dance":        "The theme is dance, party and celebration",
    "devotion":     "The theme is bhakti, devotion and god",
    "longing":      "The theme is missing someone and longing to see them",
    "friendship":   "The theme is strong friendship and friends",
    "celebration":  "The theme is festival, marriage and celebration",
    "motivation":   "The theme is motivation, success and encouragement",
    "nostalgia":    "The theme is memories of old times and nostalgia",
    "anger":        "The theme is anger, fight and ego",
    "yearning":     "The theme is deep yearning, desire and wanting someone",
    "life":         "The theme is general life, philosophy and feelings"
}

print("[INFO] Computing label embeddings...")
MOOD_LABEL_EMBS = {
    mood: MODEL.encode(text, normalize_embeddings=True)
    for mood, text in MOOD_LABEL_TEXTS.items()
}
THEME_LABEL_EMBS = {
    theme: MODEL.encode(text, normalize_embeddings=True)
    for theme, text in THEME_LABEL_TEXTS.items()
}

# ========= UTILS =========

def load_processed_keys(output_file):
    processed = set()
    if not os.path.exists(output_file):
        return processed

    print(f"[INFO] Loading already enriched records from {output_file} ...")
    with open(output_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = rec.get("song_url") or rec.get("source_url") \
                  or f"{rec.get('song_title','')}|{rec.get('movie_title','')}"
            processed.add(key)
    print(f"[INFO] Already enriched: {len(processed)} songs")
    return processed


def derive_decade(movie_year):
    if not movie_year:
        return None
    try:
        year = int(movie_year)
        decade_start = (year // 10) * 10
        return f"{decade_start}s"
    except Exception:
        return None

# ========= YOUTUBE ENRICHMENT =========

def iso8601_duration_to_seconds(duration_str):
    if not duration_str or not duration_str.startswith("PT"):
        return None
    duration_str = duration_str[2:]
    hours = minutes = seconds = 0
    num = ""
    for ch in duration_str:
        if ch.isdigit():
            num += ch
        else:
            if ch == "H":
                hours = int(num or 0)
            elif ch == "M":
                minutes = int(num or 0)
            elif ch == "S":
                seconds = int(num or 0)
            num = ""
    return hours * 3600 + minutes * 60 + seconds


def find_youtube_video(song_title, movie_title, singers=None):
    if not YOUTUBE_API_KEY:
        return None, None, None, None

    query_parts = [song_title, movie_title, "video song"]
    if singers:
        first_singer = singers.split(",")[0].strip()
        query_parts.append(first_singer)
    query = " ".join(p for p in query_parts if p)

    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet",
        "q": query,
        "maxResults": 1,
        "type": "video",
        "videoCategoryId": "10",
    }

    try:
        resp = requests.get(YOUTUBE_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            print(f"[YT] No result for: {query}")
            return None, None, None, None

        video_id = items[0]["id"]["videoId"]
        channel_title = items[0]["snippet"].get("channelTitle")

        vid_params = {
            "key": YOUTUBE_API_KEY,
            "part": "contentDetails,statistics",
            "id": video_id
        }
        vresp = requests.get(YOUTUBE_VIDEO_URL, params=vid_params, timeout=15)
        vresp.raise_for_status()
        vdata = vresp.json()
        vitems = vdata.get("items", [])
        view_count = None
        duration_sec = None
        if vitems:
            stats = vitems[0].get("statistics", {})
            view_count_str = stats.get("viewCount")
            if view_count_str is not None:
                try:
                    view_count = int(view_count_str)
                except ValueError:
                    pass
            dur_str = vitems[0].get("contentDetails", {}).get("duration")
            if dur_str:
                duration_sec = iso8601_duration_to_seconds(dur_str)

        print(f"[YT] {song_title} / {movie_title} -> {video_id} ({channel_title})")
        return video_id, channel_title, view_count, duration_sec

    except Exception as e:
        print(f"[YT ERROR] {song_title} / {movie_title}: {e}")
        return None, None, None, None

# ========= EMBEDDING + KEYWORD CLASSIFIER =========

def classify_with_embeddings(lyrics_translit, lyrics_ta=None):
    """
    Fast, local, embedding + keyword-based classifier:
      - primary_mood
      - energy_level
      - theme_tags
      - is_family_friendly
    """

    # 1) Build text
    text = (lyrics_translit or "").strip()
    if not text and lyrics_ta:
        text = lyrics_ta

    if not text:
        return "unknown", "low", "life,life", True

    # Truncate for speed & focus
    if len(text) > 400:
        text = text[:400]

    txt_lower = text.lower()

    # 2) Keyword counts (Tamil/Tanglish)
    def count_hits(words):
        return sum(1 for w in words if w in txt_lower)

    romantic_hits = count_hits(ROMANTIC_WORDS)
    sad_hits = count_hits(SAD_WORDS)
    kuthu_hits = count_hits(KUTHU_WORDS)
    devotional_hits = count_hits(DEVOTIONAL_WORDS)
    friendship_hits = count_hits(FRIENDSHIP_WORDS)
    angry_hits = count_hits(ANGRY_WORDS)

    # 3) Strong rules first: devotional & kuthu
    if devotional_hits >= 2:
        primary_mood = "devotional"
    elif kuthu_hits >= 2:
        primary_mood = "kuthu"
    else:
        # 4) Embedding-based mood classification (soft)
        song_emb = MODEL.encode(text, normalize_embeddings=True)

        mood_scores = {}
        for mood, lbl_emb in MOOD_LABEL_EMBS.items():
            score = float(util.cos_sim(song_emb, lbl_emb))
            mood_scores[mood] = score

        # 4a) Slightly boost scores with keyword hits
        mood_scores["romantic"] += 0.03 * romantic_hits
        mood_scores["sad"] += 0.03 * sad_hits
        mood_scores["melancholic"] += 0.02 * sad_hits
        mood_scores["angry"] += 0.03 * angry_hits
        mood_scores["happy"] += 0.02 * friendship_hits
        mood_scores["inspirational"] += 0.02 * friendship_hits

        primary_mood = max(mood_scores, key=mood_scores.get)

        # 4b) Guardrail: avoid fake romantic
        if primary_mood == "romantic" and romantic_hits == 0:
            if sad_hits > 0:
                primary_mood = "sad"
            elif devotional_hits > 0:
                primary_mood = "devotional"
            elif friendship_hits > 0:
                primary_mood = "happy"

    # 5) Theme tags (top 2 from embeddings, then adjusted to mood)
    song_emb = MODEL.encode(text, normalize_embeddings=True)
    theme_scores = []
    for theme, lbl_emb in THEME_LABEL_EMBS.items():
        score = float(util.cos_sim(song_emb, lbl_emb))
        theme_scores.append((theme, score))
    theme_scores.sort(key=lambda x: x[1], reverse=True)
    top_themes = [t for t, _ in theme_scores[:3]]  # take top 3, may adjust

    # Romantic/sad-type → favor love/longing/yearning/heartbreak
    if primary_mood in ["romantic", "sad", "melancholic"]:
        forced = []
        if romantic_hits > 0:
            forced.append("love")
        if sad_hits > 0:
            forced.append("heartbreak")
        if "longing" not in forced:
            forced.append("longing")
        if "yearning" not in forced:
            forced.append("yearning")

        themes_final = []
        for t in forced + top_themes:
            if t not in themes_final and t in THEME_LABEL_TEXTS:
                themes_final.append(t)
        top_themes = themes_final[:2]

    # Devotional → ensure devotion present
    if primary_mood == "devotional":
        if "devotion" not in top_themes:
            top_themes = ["devotion"] + [t for t in top_themes if t != "devotion"]
        top_themes = top_themes[:2]

    # Kuthu / dance songs → ensure dance/celebration
    if primary_mood == "kuthu" or any(w in txt_lower for w in ["dance", "party", "gaana", "mass"]):
        force_list = []
        if "dance" not in top_themes:
            force_list.append("dance")
        if "celebration" not in top_themes:
            force_list.append("celebration")
        themes_final = []
        for t in force_list + top_themes:
            if t not in themes_final and t in THEME_LABEL_TEXTS:
                themes_final.append(t)
        top_themes = themes_final[:2]

    if not top_themes:
        top_themes = ["life", "life"]
    elif len(top_themes) == 1:
        top_themes = top_themes + ["life"]

    theme_tags = ",".join(top_themes[:2])

    # 6) Energy level (combine heuristics)
    if kuthu_hits >= 2 or any(w in txt_lower for w in ["dance", "party", "gaana", "mass", "beat"]):
        energy_level = "high"
    elif primary_mood in ["sad", "melancholic", "devotional"]:
        energy_level = "low"
    else:
        energy_level = "medium"

    # 7) Family friendly – basic for now
    is_family_friendly = True

    return primary_mood, energy_level, theme_tags, is_family_friendly

# ========= PER-RECORD ENRICHMENT (THREAD WORKER) =========

def enrich_record(rec):
    """
    Enrich a single song record (no file I/O here).
    Returns (key, enriched_record) or (None, None).
    """
    key = rec.get("song_url") or rec.get("source_url") \
          or f"{rec.get('song_title','')}|{rec.get('movie_title','')}"

    song_title = rec.get("song_title", "").strip()
    movie_title = rec.get("movie_title", "").strip()
    singers = rec.get("singer") or rec.get("singers")

    print(f"[THREAD] {song_title} / {movie_title}")

    # Normalize lyrics
    lyrics_translit = rec.get("english_lyrics") or rec.get("lyrics_translit") or ""
    lyrics_ta = rec.get("tamil_lyrics") or rec.get("lyrics_ta") or ""

    rec["lyrics_translit"] = lyrics_translit
    rec["lyrics_ta"] = lyrics_ta

    # YouTube enrichment
    youtube_video_id = rec.get("youtube_video_id")
    youtube_url = rec.get("youtube_url")
    youtube_channel = rec.get("youtube_channel")
    youtube_view_count = rec.get("youtube_view_count")
    youtube_duration_sec = rec.get("youtube_duration_sec")

    if not youtube_video_id and YOUTUBE_API_KEY:
        vid, ch, vc, dur = find_youtube_video(song_title, movie_title, singers)
        youtube_video_id = vid
        youtube_channel = ch
        youtube_view_count = vc
        youtube_duration_sec = dur
        if vid:
            youtube_url = f"https://www.youtube.com/watch?v={vid}"

    rec["youtube_video_id"] = youtube_video_id
    rec["youtube_url"] = youtube_url
    rec["youtube_channel"] = youtube_channel
    rec["youtube_view_count"] = youtube_view_count
    rec["youtube_duration_sec"] = youtube_duration_sec

    # Mood & themes
    primary_mood = rec.get("primary_mood")
    energy_level = rec.get("energy_level")
    theme_tags = rec.get("theme_tags")
    is_family_friendly = rec.get("is_family_friendly")

    if not primary_mood or not energy_level or not theme_tags or is_family_friendly is None:
        pm, el, tt, ff = classify_with_embeddings(
            lyrics_translit=lyrics_translit,
            lyrics_ta=lyrics_ta
        )
        primary_mood = pm
        energy_level = el
        theme_tags = tt
        is_family_friendly = ff

    rec["primary_mood"] = primary_mood
    rec["energy_level"] = energy_level
    rec["theme_tags"] = theme_tags
    rec["is_family_friendly"] = is_family_friendly

    # Decade
    movie_year = rec.get("movie_year")
    rec["decade"] = derive_decade(movie_year)

    return key, rec

# ========= MAIN ENRICHMENT (MULTI-THREAD) =========

def enhance_dataset(input_file=INPUT_FILE, output_file=OUTPUT_FILE, max_records=None):
    processed_keys = load_processed_keys(output_file)

    # Load songs to process
    records_to_process = []
    with open(input_file, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            key = rec.get("song_url") or rec.get("source_url") \
                  or f"{rec.get('song_title','')}|{rec.get('movie_title','')}"
            if key in processed_keys:
                continue

            records_to_process.append(rec)
            if max_records and len(records_to_process) >= max_records:
                break

    total = len(records_to_process)
    print(f"[INFO] To process this run: {total} songs")

    if total == 0:
        print("[INFO] Nothing new to process.")
        return

    mode = "a" if os.path.exists(output_file) else "w"
    count = 0

    with open(output_file, mode, encoding="utf-8") as fout:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_idx = {
                executor.submit(enrich_record, rec): idx
                for idx, rec in enumerate(records_to_process)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    key, enriched = future.result()
                except Exception as e:
                    print(f"[THREAD ERROR] record {idx} failed: {e}")
                    continue

                if not key or not enriched:
                    continue

                fout.write(json.dumps(enriched, ensure_ascii=False) + "\n")
                fout.flush()

                processed_keys.add(key)
                count += 1

                if count % LOG_EVERY == 0 or count == total:
                    print(f"[PROGRESS] {count}/{total} enriched")

    print(f"[DONE] Enriched {count} songs in this run. Output -> {output_file}")


if __name__ == "__main__":
    # Quick test on subset:
    # enhance_dataset(max_records=50)

    enhance_dataset()
