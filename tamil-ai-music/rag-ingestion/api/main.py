from fastapi import FastAPI, Query
from typing import Optional, List, Dict, Any
from fastapi import HTTPException
from src.config import ENABLE_WEB_RESOLUTION

from src.qdrant_updates import patch_song_payload
from src.youtube_resolver import youtube_search_url

from src.web_music_resolver import resolve_from_web
from src.qdrant_utils import update_song_payload
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from qdrant_client import QdrantClient
from src.config import QDRANT_URL, COLLECTION

from src.web_music_resolver import resolve_from_web

import os
DISABLE_WEB_RESOLVER = os.getenv("DISABLE_WEB_RESOLVER", "0") == "1"



from src.search_qdrant import search_songs
from src.playlist_builder import (
    build_playlist_from_seed,
    build_playlist_from_query
)


def _upsert_music_meta_to_qdrant(meta_map: dict[str, dict]) -> int:
    if not meta_map:
        return 0

    client = QdrantClient(url=QDRANT_URL)
    updated_songs = 0

    for song_id, payload_updates in meta_map.items():
        client.set_payload(
            collection_name=COLLECTION,
            payload=payload_updates,
            points=Filter(
                must=[
                    FieldCondition(
                        key="song_id",
                        match=MatchValue(value=song_id)
                    )
                ]
            ),
        )
        updated_songs += 1

    return updated_songs

def _resolve_and_upsert_music_meta(items: list[dict]) -> int:
    if not ENABLE_WEB_RESOLUTION:
        return 0

    candidates = []
    for it in items:
        if it.get("genre") and it.get("rhythm"):
            continue
        if not it.get("song_id") or not it.get("title"):
            continue
        candidates.append(it)

    # ✅ cap to avoid 20 web calls per request
    candidates = candidates[:3]

    if not candidates:
        return 0

    meta_map = {}
    for it in candidates:
        try:
            meta = resolve_from_web(it)
        except Exception:
            meta = None

        if not meta:
            continue

        meta_map[it["song_id"]] = {
            "genre": meta.get("genre"),
            "rhythm": meta.get("rhythm"),
            "mood_web": meta.get("mood"),
            "meta_source": meta.get("source"),
            "meta_confidence": meta.get("confidence"),
        }

    if not meta_map:
        return 0

    try:
        return _upsert_music_meta_to_qdrant(meta_map)
    except Exception:
        return 0

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
@app.get("/playlist/seed/{seed_song_id}")
def playlist_from_seed(seed_song_id: str, k: int = Query(20, ge=1, le=50)):
    data = build_playlist_from_seed(seed_song_id=seed_song_id, limit_songs=k)

    if not data.get("ok"):
        # tests accept 400/404/422; 404 is best
        raise HTTPException(status_code=404, detail=data.get("error", "Seed not found"))
    
# ✅ AFTER playlist is built:
    url_map = _extract_url_map(data.get("items", []))
    updated = _upsert_youtube_urls_to_qdrant(url_map)
    data["youtube_urls_saved"] = updated
    meta_updated = _resolve_and_upsert_music_meta(data.get("items", []))
    data["music_meta_saved"] = meta_updated
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
    meta_updated = _resolve_and_upsert_music_meta(data.get("items", []))
    data["music_meta_saved"] = meta_updated
    
    return data

@app.get("/player/query")
def player_query(
    q: str = Query(...),
    mood: Optional[str] = Query(None),
    k: int = Query(20),
):
    return playlist_query(q=q, mood=mood, k=k)


@app.get("/player/seed/{seed_song_id}")
def player_seed(
    seed_song_id: str,
    k: int = Query(20),
):
    return playlist_from_seed(seed_song_id=seed_song_id, k=k)

@app.post("/enrich/youtube")
def enrich_youtube_urls(song_ids: List[str] = Body(..., embed=True)):
    """
    UI calls this after it shows playlist.
    For each song_id, if youtube_url missing, resolve + upsert to Qdrant.
    """
    if not song_ids:
        return {"ok": True, "requested": 0, "updated": 0}

    # 1) fetch existing payload for these songs (optional but recommended)
    # If you already have title/movie in UI items, you can skip fetching.
    # We'll assume you can resolve using song_id -> payload in Qdrant
    items = fetch_items_by_song_ids(song_ids)  # <-- implement or reuse existing helper

    # 2) Build url_map only for missing youtube_url
    url_map = _extract_url_map(items)  # should only produce missing ones
    updated = _upsert_youtube_urls_to_qdrant(url_map)

    return {"ok": True, "requested": len(song_ids), "updated": updated}