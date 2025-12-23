import json
import requests
from typing import Optional, Dict, Any

OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3"

# Keep enums tight so results are consistent
ALLOWED_MOODS = ["romantic", "happy", "melancholic", "sad", "kuthu", "angry", "devotional", "inspirational", "unknown"]
ALLOWED_RHYTHMS = ["slow", "mid", "fast", "unknown"]
ALLOWED_GENRES = ["love", "dance", "devotion", "friendship", "heartbreak", "nostalgia", "celebration", "anger", "unknown"]

def llm_classify_song_meta(title: str, movie: str | None = None, year: str | None = None) -> Optional[Dict[str, Any]]:
    prompt = f"""
You are classifying a Tamil song using ONLY the metadata given.
Return JSON ONLY. No markdown.

Fields:
- mood: one of {ALLOWED_MOODS}
- rhythm: one of {ALLOWED_RHYTHMS}
- genre: one of {ALLOWED_GENRES}
- confidence: number 0.0 to 1.0

Rules:
- If unsure, set "unknown" and low confidence.
- Use title/movie/year hints only.

Song:
title: {title}
movie: {movie or ""}
year: {year or ""}
""".strip()

    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    text = (data.get("message") or {}).get("content", "").strip()

    # Parse JSON safely
    try:
        out = json.loads(text)
    except Exception:
        return None

    # Hard validate
    mood = out.get("mood", "unknown")
    rhythm = out.get("rhythm", "unknown")
    genre = out.get("genre", "unknown")
    conf = out.get("confidence", 0.0)

    if mood not in ALLOWED_MOODS: mood = "unknown"
    if rhythm not in ALLOWED_RHYTHMS: rhythm = "unknown"
    if genre not in ALLOWED_GENRES: genre = "unknown"
    try:
        conf = float(conf)
    except Exception:
        conf = 0.0

    return {"mood": mood, "rhythm": rhythm, "genre": genre, "confidence": max(0.0, min(1.0, conf))}