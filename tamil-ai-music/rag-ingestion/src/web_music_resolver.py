# src/web_music_resolver.py

from __future__ import annotations
from typing import Dict, Optional, List
import requests

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
MB_SEARCH = "https://musicbrainz.org/ws/2/recording/"

# Very light heuristics (you can expand later)
GENRE_TO_MOOD = {
    "love song": "romantic",
    "romance": "romantic",
    "melody": "romantic",
    "sad song": "sad",
    "devotional song": "devotional",
    "folk": "happy",
    "dance": "kuthu",
    "hip hop": "kuthu",
    "rap": "kuthu",
    "rock": "happy",
}

RHYTHM_KEYWORDS = {
    "dance": "fast",
    "hip hop": "fast",
    "rap": "fast",
    "rock": "medium",
    "folk": "medium",
    "melody": "slow",
    "love": "slow",
    "romance": "slow",
    "sad": "slow",
    "devotional": "slow",
}

def _infer_rhythm(genres: List[str]) -> str:
    g = " ".join(genres)
    for k, v in RHYTHM_KEYWORDS.items():
        if k in g:
            return v
    return "unknown"

def _infer_mood(genres: List[str]) -> Optional[str]:
    g = " ".join(genres)
    for k, v in GENRE_TO_MOOD.items():
        if k in g:
            return v
    return None

def _resolve_from_wikidata(title: str) -> Optional[Dict]:
    if not title:
        return None

    # Wikidata labels are messy; this works only sometimes, but it’s cheap
    query = f"""
    SELECT ?genreLabel WHERE {{
      ?song rdfs:label "{title}"@en .
      ?song wdt:P136 ?genre .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }} LIMIT 5
    """

    r = requests.get(
        WIKIDATA_ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"User-Agent": "TamilMusicAI/1.0 (+https://example.com)"}
    )
    if r.status_code != 200:
        return None

    rows = r.json().get("results", {}).get("bindings", [])
    if not rows:
        return None

    genres = [x["genreLabel"]["value"].lower() for x in rows if "genreLabel" in x]
    if not genres:
        return None

    return {
        "genre": genres[0],
        "rhythm": _infer_rhythm(genres),
        "mood": _infer_mood(genres),
        "source": "wikidata",
        "confidence": 0.65,
    }

def _resolve_from_musicbrainz(title: str, artist: str | None = None):
    try:
        r = requests.get(
            "https://musicbrainz.org/ws/2/recording/",
            params={
                "query": f'recording:"{title}"' + (f' AND artist:"{artist}"' if artist else ""),
                "fmt": "json",
                "limit": 1,
            },
            headers={"User-Agent": "TamilMusicAI/1.0 (+https://example.com)"},
            timeout=6,   # ✅ IMPORTANT
        )
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None
    
    
def _resolve_from_lyrics_text(song: Dict) -> Optional[Dict]:
    text = " ".join([
        song.get("title", "") or "",
        song.get("movie", "") or "",
        song.get("lyrics_ta", "") or "",
        song.get("lyrics_en", "") or "",
        song.get("best_chunk", "") or "",
    ]).lower()

    for key, rhythm in RHYTHM_KEYWORDS.items():
        if key in text:
            return {
                "genre": key,
                "rhythm": rhythm,
                "mood": GENRE_TO_MOOD.get(key),
                "source": "lyrics_text",
                "confidence": 0.35,
            }
    return None

def resolve_from_web(song: Dict) -> Optional[Dict]:
    """
    Try web-based resolution for genre/rhythm/mood.
    Returns None if nothing useful found.
    """
    title = song.get("title")
    # artist is unknown in your dataset; you can add later
    artist = None

    # 1) Wikidata
    out = _resolve_from_wikidata(title)
    if out:
        return out

    # 2) MusicBrainz
    out = _resolve_from_musicbrainz(title, artist=artist)
    if out:
        return out

    # 3) Keyword fallback
    return _resolve_from_lyrics_text(song)
