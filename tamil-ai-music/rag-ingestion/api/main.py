from fastapi import FastAPI, Query
from typing import Optional, List, Dict, Any
from fastapi import HTTPException

from src.qdrant_updates import patch_song_payload
from src.youtube_resolver import youtube_search_url

from src.search_qdrant import search_songs
from src.playlist_builder import (
    build_playlist_from_seed,
    build_playlist_from_query
)

app = FastAPI(
    title="Tamil AI Music Engine",
    description="Semantic search & playlist generation over Tamil song lyrics",
    version="1.0.0"
)


def _ensure_youtube_urls(items: list[dict]) -> list[dict]:
    for it in items:
        if not it.get("youtube_url"):
            url = youtube_search_url(it.get("title") or "", it.get("movie"), it.get("year"))
            it["youtube_url"] = url
            it["youtube_source"] = "search"
            if it.get("song_id"):
                patch_song_payload(it["song_id"], {
                    "youtube_url": url,
                    "youtube_source": "search",
                })
    return items

# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return {"ok": True, "status": "ok"}


# -------------------------
# Search
# -------------------------
@app.get("/search")
def search(
    q: str = Query(..., description="Search query"),
    mood: Optional[str] = Query(None, description="Mood filter"),
    k: int = Query(10, ge=1, le=50),
):
    # IMPORTANT: use the param name search_songs expects (likely `limit`)
    hits = search_songs(query=q, mood=mood, k=k)

    return {
        "ok": True,
        "query": q,
        "mood": mood,
        "count": len(hits),
        "items": hits,
    }
    
    
# -------------------------
# Playlist from seed song
# -------------------------
@app.get("/playlist/seed/{song_id}")
def playlist_from_seed(song_id: str, k: int = Query(20, ge=1, le=50)):
    data = build_playlist_from_seed(seed_song_id=song_id, limit_songs=k)

    if not data.get("ok"):
        # tests accept 400/404/422; 404 is best
        raise HTTPException(status_code=404, detail=data.get("error", "Seed not found"))

    return data


# -------------------------
# Playlist from text query
# -------------------------
@app.get("/playlist/query")
def playlist_query(q: str, mood: str | None = None, k: int = 20):
    data = build_playlist_from_query(query=q, mood=mood, k=k)

    # If the builder returns a list, wrap it into a consistent dict contract
    if isinstance(data, list):
        return {
            "ok": True,
            "query": q,
            "mood": mood,
            "count": len(data),
            "items": data,
        }

    if "ok" not in data:
        data["ok"] = True
    return data

