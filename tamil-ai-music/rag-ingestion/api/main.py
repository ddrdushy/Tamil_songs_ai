
from typing import Optional, List, Dict, Any
from src.config import ENABLE_WEB_RESOLUTION

from src.qdrant_updates import patch_song_payload
from src.youtube_resolver import youtube_search_url
from src.qdrant_read import fetch_items_by_song_ids
from src.web_music_resolver import resolve_from_web
from src.qdrant_utils import update_song_payload
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, FilterSelector
from qdrant_client.http import models as rest
from datetime import datetime
from fastapi import FastAPI, Query, HTTPException, Body,BackgroundTasks
from urllib.parse import urlparse



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
    updated = 0

    for song_id, payload_updates in meta_map.items():
        selector = rest.FilterSelector(
            filter=rest.Filter(
                must=[
                    rest.FieldCondition(
                        key="song_id",
                        match=rest.MatchValue(value=song_id),
                    )
                ]
            )
        )

        client.set_payload(
            collection_name=COLLECTION,
            payload=payload_updates,
            points=selector,   # ✅ same fix
        )
        updated += 1

    return updated

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

def _is_good_youtube_url(url: str | None) -> bool:
    if not url:
        return False
    u = url.strip()

    # BAD: search results placeholder
    if "youtube.com/results" in u and "search_query=" in u:
        return False

    # GOOD: watch or youtu.be
    return ("youtube.com/watch" in u) or ("youtu.be/" in u)


def _upsert_youtube_urls_to_qdrant(url_map: dict[str, str]) -> int:
    if not url_map:
        return 0

    client = QdrantClient(url=QDRANT_URL)
    updated = 0

    for song_id, youtube_url in url_map.items():
        if not song_id or not youtube_url:
            continue
        is_good = _is_good_youtube_url(youtube_url)

        # ✅ THIS is the payload_updates you asked about
        payload_updates = {
            "youtube_url": youtube_url,
            "youtube_url_status": "resolved" if is_good else "placeholder",
            "youtube_url_source": "youtube_resolver" if is_good else "search_placeholder",
            "youtube_url_needs_refresh": not is_good,
            "youtube_url_resolved_at": datetime.utcnow().isoformat() + "Z",
            "youtube_url_resolver_version": "v2",
        }
        selector = FilterSelector(
        filter=Filter(
            must=[
                FieldCondition(
                    key="song_id",
                    match=MatchValue(value=song_id),
                )
            ]
        )
    )

        client.set_payload(
            collection_name=COLLECTION,
            payload=payload_updates,
            points=selector,   # ✅ correct for your qdrant-client version
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


@app.post("/player/items-by-song-ids")
def items_by_song_ids(payload: dict = Body(...)):
    song_ids = payload.get("song_ids", [])
    if not isinstance(song_ids, list) or not song_ids:
        return {"ok": True, "items": []}

    # TODO: implement this function in your existing qdrant/search module
    items = fetch_items_by_song_ids(song_ids)

    return {"ok": True, "count": len(items), "items": items}


@app.post("/player/enrich-youtube-urls")
def enrich_youtube_urls(payload: dict = Body(...)):
    """
    Body: { "song_ids": ["id1", "id2", ...] }

    Resolves missing youtube_url for provided songs and saves into Qdrant.
    Returns updated items so UI can merge immediately.
    """
    song_ids = payload.get("song_ids") or []
    if not isinstance(song_ids, list) or not song_ids:
        raise HTTPException(status_code=422, detail="song_ids must be a non-empty list")

    # 1) fetch minimal payloads from Qdrant
    items = fetch_items_by_song_ids(song_ids)

    # 2) build url_map ONLY for missing youtube_url
    # expected: { song_id: "https://www.youtube.com/watch?v=..." }
    url_map = {}
    for it in items:
        sid = it.get("song_id")
        if not sid:
            continue
        if it.get("youtube_url"):
            continue  # already present
        title = (it.get("title") or "").strip()
        movie = (it.get("movie") or "").strip()

        # your resolver should accept a string query
        if title:
            q = f"{title} {movie}".strip()
            url = youtube_search_url(q)   # from src.youtube_resolver
            if url:
                url_map[sid] = url

    # 3) upsert into qdrant
    updated = _upsert_youtube_urls_to_qdrant(url_map)

    # 4) return updated items to UI
    updated_items = []
    for it in items:
        sid = it.get("song_id")
        if sid in url_map and not it.get("youtube_url"):
            it = {**it, "youtube_url": url_map[sid]}
        updated_items.append(it)

    return {"ok": True, "updated": updated, "items": updated_items}