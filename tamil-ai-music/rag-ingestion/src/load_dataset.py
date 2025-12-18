import json
import hashlib
from typing import Iterator, Dict, Any

from src.preprocess import (
    make_song_id,
    pick_lyrics_field,
    clean_lyrics,
    lyrics_hash,
)


def meta_hash(row: Dict[str, Any]) -> str:
    """
    Hash only metadata fields (no lyrics).
    """
    keys = [
        "song_title",
        "singer",
        "movie_title",
        "movie_year",
        "primary_mood",
        "theme_tags",
        "decade",
    ]
    base = "|".join(str(row.get(k, "")).strip().lower() for k in keys)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def load_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def iter_songs(path: str):
    """
    Yields normalized song records ready for ingestion decisions.
    """
    for row in load_jsonl(path):
        song_id = make_song_id(
            title=row.get("song_title", ""),
            singer=row.get("singer", ""),
            movie=row.get("movie_title", ""),
            year=row.get("movie_year", ""),
        )

        raw_lyrics = pick_lyrics_field(row)
        cleaned = clean_lyrics(raw_lyrics)

        yield {
            "song_id": song_id,
            "lyrics": cleaned,
            "lyrics_hash": lyrics_hash(cleaned),
            "meta_hash": meta_hash(row),
            "metadata": {
                "title": row.get("song_title"),
                "singer": row.get("singer"),
                "movie": row.get("movie_title"),
                "year": row.get("movie_year"),
                "mood": row.get("primary_mood"),
                "themes": row.get("theme_tags"),
                "decade": row.get("decade"),
                "youtube_video_id": row.get("youtube_video_id"),
            }
        }
