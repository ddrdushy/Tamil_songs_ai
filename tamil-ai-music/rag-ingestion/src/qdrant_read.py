# src/qdrant_read.py
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from src.config import QDRANT_URL, COLLECTION


def fetch_items_by_song_ids(song_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch minimal payload for given song_ids from Qdrant.
    Used for async enrichment (YouTube, web, LLM).
    """
    if not song_ids:
        return []

    client = QdrantClient(url=QDRANT_URL)

    results = client.scroll(
        collection_name=COLLECTION,
        scroll_filter={
            "must": [
                {
                    "key": "song_id",
                    "match": {"any": song_ids},
                }
            ]
        },
        with_payload=True,
        limit=len(song_ids) * 3,  # multiple chunks per song
    )[0]

    seen = {}
    for p in results:
        payload = p.payload or {}
        sid = payload.get("song_id")
        if not sid or sid in seen:
            continue

        seen[sid] = {
            "song_id": sid,
            "title": payload.get("title"),
            "movie": payload.get("movie"),
            "year": payload.get("year"),
            "youtube_url": payload.get("youtube_url"),
        }

    return list(seen.values())
