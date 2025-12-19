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

def _extract_url_map(items: list[dict]) -> dict[str, str]:
    """
    Build {song_id: youtube_url} from playlist items.
    Assumes you've already populated youtube_url somewhere.
    """
    out = {}
    for it in items or []:
        sid = it.get("song_id")
        url = it.get("youtube_url")
        if sid and url:
            out[sid] = url
    return out


def _upsert_youtube_urls_to_qdrant(url_map: dict[str, str]) -> int:
    """
    Writes youtube_url into Qdrant payload for those song_id points.
    Returns number of song_ids updated.
    """
    if not url_map:
        return 0

    client = QdrantClient(url=QDRANT_URL)

    # IMPORTANT:
    # This assumes your points have payload field "song_id"
    # and you want to update payload field "youtube_url".
    #
    # We update by filter (song_id == X) so we don't need point IDs.
    updated = 0
    for song_id, url in url_map.items():
        client.set_payload(
            collection_name=COLLECTION,
            payload={"youtube_url": url},
            points=None,
            filter={
                "must": [{"key": "song_id", "match": {"value": song_id}}],
            },
        )
        updated += 1

    return updated

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
    
# ✅ AFTER playlist is built:
    url_map = _extract_url_map(data.get("items", []))
    updated = _upsert_youtube_urls_to_qdrant(url_map)
    data["youtube_urls_saved"] = updated
    
    return data


# -------------------------
# Playlist from text query
# -------------------------
@app.get("/playlist/query")
def playlist_query(
    q: str = Query(...),
    mood: Optional[str] = Query(None),
    k: int = Query(20, ge=1, le=50),
):
    items = build_playlist_from_query(query=q, mood=mood, k=k)

    # build_playlist_from_query currently returns list[dict]
    if not isinstance(items, list):
        raise HTTPException(status_code=500, detail="build_playlist_from_query must return a list")

    data = {
        "ok": True,
        "query": q,
        "mood": mood,
        "count": len(items),
        "items": items,
    }

    # ✅ AFTER playlist is built:
    url_map = _extract_url_map(data["items"])
    updated = _upsert_youtube_urls_to_qdrant(url_map)
    data["youtube_urls_saved"] = updated

    return data

@app.get("/player/query")
def player_query(
    q: str = Query(..., description="Search query"),
    mood: Optional[str] = Query(None, description="Mood filter"),
    k: int = Query(20, ge=1, le=50),
):
    # reuse existing function
    return playlist_query(q=q, mood=mood, k=k)


@app.get("/player/seed/{seed_song_id}")
def player_seed(
    seed_song_id: str,
    k: int = Query(20, ge=1, le=50),
):
    # reuse existing function
    return playlist_from_seed(seed_song_id=seed_song_id, k=k)