#!/usr/bin/env python3
"""
One-time updater: infer mood/genre/rhythm via local Ollama (smollm2:135m)
and upsert into Qdrant for ALL chunks of each song_id.

Usage examples:
  python scripts/update_song_meta_ollama.py \
    --qdrant-url http://localhost:6333 \
    --collection tamil_songs \
    --model smollm2:135m \
    --limit 2000 \
    --force

  # safer: write only to *_llm fields (default), do not overwrite mood/genre/rhythm unless missing/unknown
  python scripts/update_song_meta_ollama.py --collection tamil_songs

  # dry run (no qdrant updates)
  python scripts/update_song_meta_ollama.py --collection tamil_songs --dry-run
"""

import argparse
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


# -------------------------
# Helpers: text extraction
# -------------------------

LIKELY_LYRICS_FIELDS = [
    "lyrics_tamil",
    "lyrics_tanglish",
    "lyrics",
    "lyrics_text",
    "chunk_text",
    "text",
    "content",
]

def _clean(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s

def _extract_lyrics_excerpt(payload: Dict[str, Any], max_chars: int = 500) -> str:
    """
    Try to extract lyrics-ish text from payload.
    Prefers explicit lyrics fields, falls back to chunk_text.
    """
    parts: List[str] = []
    for k in LIKELY_LYRICS_FIELDS:
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())

    if not parts:
        return ""

    # Join but keep it short
    joined = _clean(" \n".join(parts))
    return joined[:max_chars]

def _build_llm_context(payload: Dict[str, Any], max_lyrics_chars: int = 500) -> str:
    title = payload.get("title") or ""
    movie = payload.get("movie") or ""
    year = payload.get("year") or ""
    artist = payload.get("artist") or payload.get("singer") or ""

    lyrics = _extract_lyrics_excerpt(payload, max_chars=max_lyrics_chars)

    ctx = []
    if title: ctx.append(f"Title: {title}")
    if movie: ctx.append(f"Movie: {movie}")
    if year: ctx.append(f"Year: {year}")
    if artist: ctx.append(f"Artist: {artist}")
    if lyrics: ctx.append(f"Lyrics excerpt (may contain Tamil/Tanglish): {lyrics}")

    return "\n".join(ctx).strip()


# -------------------------
# Ollama call + parsing
# -------------------------

MOOD_LABELS = [
    "romantic",
    "happy",
    "sad",
    "melancholic",
    "kuthu",
    "devotional",
    "angry",
    "inspirational",
    "moivation",
    "unknown",
]

GENRE_LABELS = [
    "melody",
    "romantic_melody",
    "dance",
    "kuthu",
    "folk",
    "gaana",
    "devotional",
    "classical",
    "hip_hop",
    "rock",
    "pop",
    "item",
    "unknown",
]

RHYTHM_LABELS = [
    "slow",
    "mid",
    "fast",
    "unknown",
]


def _post_with_retry(url: str, payload: dict, timeout_s: int, retries: int = 3, backoff_s: float = 2.0):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return requests.post(url, json=payload, timeout=timeout_s)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            time.sleep(backoff_s * attempt)
    raise last_err

def _ollama_classify(
    ollama_url: str,
    model: str,
    ctx: str,
    timeout_s: int = 600,
    retries: int = 3
) -> Optional[Dict[str, Any]]:
    """
    Calls Ollama and expects STRICT JSON output.
    Returns dict: {mood, genre, rhythm, confidence}
    """
    system = (
        "You are a music metadata classifier for Tamil songs.\n"
        "Given title/movie/year and a short lyrics excerpt, classify:\n"
        f"- mood: one of {MOOD_LABELS}\n"
        f"- genre: one of {GENRE_LABELS}\n"
        f"- rhythm: one of {RHYTHM_LABELS} (tempo)\n"
        "Return ONLY valid JSON (no markdown, no commentary).\n"
        "If unsure, use 'unknown' and lower confidence.\n"
    )

    user = (
        "Classify this song.\n\n"
        "Output JSON schema:\n"
        '{"mood":"...", "genre":"...", "rhythm":"...", "confidence":0.0}\n\n'
        f"Song info:\n{ctx}\n"
    )

    # Use /api/chat when available (more controllable), fallback to /api/generate if needed.
    chat_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": 0.2, "num_predict": 120},
    }

    url = f"{ollama_url.rstrip('/')}/api/chat"
    r = _post_with_retry(url, chat_payload, timeout_s=timeout_s, retries=retries)
    r.raise_for_status()
    
    data = r.json()

    # ✅ RAW model text (this is what you want to see)
    raw_text = (data.get("message") or {}).get("content", "")
    if debug:
        print("\n" + "=" * 80)
        print(f"[LLM RAW OUTPUT]")
        print(raw_text)
        print("=" * 80 + "\n")

    
    if r.status_code != 200:
        return None

    content = (r.json().get("message") or {}).get("content") or ""
    content = content.strip()

    # Extract the first JSON object from the response
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return None

    try:
        obj = json.loads(m.group(0))
    except Exception:
        return None

    # Normalize / validate
    mood = obj.get("mood", "unknown")
    genre = obj.get("genre", "unknown")
    rhythm = obj.get("rhythm", "unknown")
    conf = obj.get("confidence", 0.3)

    if mood not in MOOD_LABELS:
        mood = "unknown"
    if genre not in GENRE_LABELS:
        genre = "unknown"
    if rhythm not in RHYTHM_LABELS:
        rhythm = "unknown"

    try:
        conf = float(conf)
    except Exception:
        conf = 0.3
    conf = max(0.0, min(conf, 1.0))

    return {"mood": mood, "genre": genre, "rhythm": rhythm, "confidence": conf}


# -------------------------
# Qdrant scan + upsert
# -------------------------

def _iter_unique_songs(
    client: QdrantClient,
    collection: str,
    page_size: int = 256,
    limit_songs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Scroll Qdrant points, dedup by song_id, return representative payload per song.
    """
    offset = None
    seen: Dict[str, Dict[str, Any]] = {}

    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=page_size,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for p in points:
            payload = p.payload or {}
            sid = payload.get("song_id")
            if not sid:
                continue
            if sid in seen:
                # keep first seen (or you can prefer richer one if you want)
                continue
            seen[sid] = payload

            if limit_songs and len(seen) >= limit_songs:
                return [{"song_id": k, "payload": v} for k, v in seen.items()]

        if not next_offset:
            break
        offset = next_offset

    return [{"song_id": k, "payload": v} for k, v in seen.items()]

def _needs_update(
    payload: Dict[str, Any],
    force: bool,
) -> bool:
    """
    Default behavior: update only if missing/unknown.
    If force=True: update everything.
    """
    if force:
        return True

    # Only if missing or unknown
    mood = (payload.get("mood_llm") or payload.get("mood") or "").strip().lower()
    genre = (payload.get("genre_llm") or payload.get("genre") or "").strip().lower()
    rhythm = (payload.get("rhythm_llm") or payload.get("rhythm") or "").strip().lower()

    def bad(x: str) -> bool:
        return (not x) or (x == "unknown")

    return bad(mood) or bad(genre) or bad(rhythm)

def _upsert_song_payload_by_song_id(
    client: QdrantClient,
    collection: str,
    song_id: str,
    payload_updates: Dict[str, Any],
) -> None:
    """
    Update ALL chunks for a song_id using FilterSelector.
    This avoids the 'missing points' / 'unknown filter arg' issues.
    """
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
        collection_name=collection,
        payload=payload_updates,
        points=selector,
    )


# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qdrant-url", default=os.getenv("QDRANT_URL", "http://localhost:6333"))
    ap.add_argument("--collection", required=True)
    ap.add_argument("--ollama-url", default=os.getenv("OLLAMA_URL", "http://localhost:11434"))
    ap.add_argument("--model", default=os.getenv("OLLAMA_MODEL", "smollm2:135m"))

    ap.add_argument("--page-size", type=int, default=256)
    ap.add_argument("--limit", type=int, default=0, help="Limit number of unique songs (0 = all)")
    ap.add_argument("--sleep-ms", type=int, default=0, help="Sleep between LLM calls")

    ap.add_argument("--force", action="store_true", help="Overwrite even if mood/genre/rhythm already present")
    ap.add_argument("--write-canonical", action="store_true",
                    help="Also overwrite canonical fields mood/genre/rhythm (default writes *_llm only)")
    ap.add_argument("--dry-run", action="store_true")

    ap.add_argument("--checkpoint", default="song_meta_checkpoint.jsonl",
                    help="JSONL file to store processed song_ids (resume support)")
    
    ap.add_argument("--debug", action="store_true", help="Print raw LLM output + parsed result")
    ap.add_argument("--debug-every", type=int, default=1, help="Print debug every N songs")

    ap.add_argument(
    "--print-raw",
    action="store_true",
    help="Print raw LLM output before JSON parsing",
    )

    ap.add_argument(
        "--max-songs",
        type=int,
        default=None,
        help="Process only N unique songs (debug runs)",
    )
    args = ap.parse_args()
    max_songs = args.max_songs or args.limit
    limit_songs = args.limit if args.limit and args.limit > 0 else None

    client = QdrantClient(url=args.qdrant_url)

    # Load checkpoint (already processed)
    done = set()
    if args.checkpoint and os.path.exists(args.checkpoint):
        with open(args.checkpoint, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sid = rec.get("song_id")
                    if sid:
                        done.add(sid)
                except Exception:
                    pass

    songs = _iter_unique_songs(
        client=client,
        collection=args.collection,
        page_size=args.page_size,
        limit_songs=limit_songs,
    )

    print(f"Unique songs found: {len(songs)} (checkpointed: {len(done)})")

    updated = 0
    skipped = 0
    failed = 0

    ckpt_f = open(args.checkpoint, "a", encoding="utf-8") if args.checkpoint else None
    

    try:
        for idx, row in enumerate(songs, 1):
            song_id = row["song_id"]
            payload = row["payload"]

            if song_id in done:
                skipped += 1
                continue

            if not _needs_update(payload, force=args.force):
                skipped += 1
                if ckpt_f:
                    ckpt_f.write(json.dumps({"song_id": song_id, "status": "skipped"}) + "\n")
                    ckpt_f.flush()
                continue

            ctx = _build_llm_context(payload, max_lyrics_chars=500)
            if not ctx:
                failed += 1
                if ckpt_f:
                    ckpt_f.write(json.dumps({"song_id": song_id, "status": "no_text"}) + "\n")
                    ckpt_f.flush()
                continue

            meta = _ollama_classify(
                ollama_url=args.ollama_url,
                model=args.model,
                ctx=ctx,
                timeout_s=60,
                debug=args.debug,
                
            )

            if not meta:
                failed += 1
                if ckpt_f:
                    ckpt_f.write(json.dumps({"song_id": song_id, "status": "llm_failed"}) + "\n")
                    ckpt_f.flush()
                continue

            now = datetime.utcnow().isoformat() + "Z"
            updates = {
                "mood_llm": meta["mood"],
                "genre_llm": meta["genre"],
                "rhythm_llm": meta["rhythm"],
                "meta_source": f"ollama:{args.model}",
                "meta_confidence": meta["confidence"],
                "meta_updated_at": now,
            }

            # Optionally overwrite canonical fields too
            if args.write_canonical:
                updates["mood"] = meta["mood"]
                updates["genre"] = meta["genre"]
                updates["rhythm"] = meta["rhythm"]

            if args.dry_run:
                updated += 1
            else:
                _upsert_song_payload_by_song_id(
                    client=client,
                    collection=args.collection,
                    song_id=song_id,
                    payload_updates=updates,
                )
                updated += 1

            if ckpt_f:
                ckpt_f.write(json.dumps({"song_id": song_id, "status": "updated", **updates}) + "\n")
                ckpt_f.flush()

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

            if idx % 25 == 0:
                print(f"[{idx}/{len(songs)}] updated={updated} skipped={skipped} failed={failed}")
                
            if max_songs and processed >= max_songs:
                break

    finally:
        if ckpt_f:
            ckpt_f.close()

    print(f"Done. updated={updated}, skipped={skipped}, failed={failed}")
    if args.dry_run:
        print("Dry-run mode ON (no Qdrant writes).")


if __name__ == "__main__":
    main()
