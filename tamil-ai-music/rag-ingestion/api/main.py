from fastapi import FastAPI, Query
from typing import Optional, List, Dict, Any

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

# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------
# Search
# -------------------------
@app.get("/search")
def search(
    q: str = Query(..., description="Search query"),
    mood: Optional[str] = Query(None, description="Mood filter"),
    k: int = Query(10, ge=1, le=50)
):
    hits = search_songs(query=q, mood=mood, k=k)

    return {
        "query": q,
        "mood": mood,
        "results": hits
    }


# -------------------------
# Playlist from seed song
# -------------------------
@app.get("/playlist/seed/{song_id}")
def playlist_from_seed(
    song_id: str,
    k: int = Query(15, ge=5, le=50)
):
    playlist = build_playlist_from_seed(song_id=song_id, k=k)

    return {
        "seed_song_id": song_id,
        "count": len(playlist),
        "playlist": playlist
    }


# -------------------------
# Playlist from text query
# -------------------------
@app.get("/playlist/query")
def playlist_from_query(
    q: str = Query(..., description="Seed query"),
    mood: Optional[str] = Query(None),
    k: int = Query(15, ge=5, le=50)
):
    playlist = build_playlist_from_query(query=q, mood=mood, k=k)

    return {
        "query": q,
        "mood": mood,
        "count": len(playlist),
        "playlist": playlist
    }
