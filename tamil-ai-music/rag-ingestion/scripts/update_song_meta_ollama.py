#!/usr/bin/env python3
"""
Update mood/genre/rhythm for songs stored in Qdrant using a LOCAL Ollama model.

- Dedupes by payload["song_id"] (since multiple chunks per song)
- Uses up to N chars of lyrics (Tamil/Tanglish/English) to classify
- Writes payload updates back to ALL Qdrant points that share that song_id
- Compatible with older qdrant-client versions (doesn't require set_payload(filter=...))
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from qdrant_client import QdrantClient

# -----------------------------
# Helpers
# -----------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def pick_lyrics(payload: Dict[str, Any], max_chars: int = 500) -> str:
    """
    Pick best available lyrics text from payload.
    Supports common keys seen in your dataset.
    """
    candidates = [
        payload.get("lyrics_tamil"),
        payload.get("lyrics_tanglish"),
        payload.get("lyrics_text"),
        payload.get("lyrics"),
        payload.get("text"),
        payload.get("chunk_text"),
        payload.get("content"),
    ]
    txt = ""
    for c in candidates:
        if isinstance(c, str) and c.strip():
            txt = c.strip()
            break
    txt = re.sub(r"\s+", " ", txt).strip()
    if len(txt) > max_chars:
        txt = txt[:max_chars].rstrip() + "…"
    return txt


def _extract_json_from_text(s: str) -> Optional[dict]:
    """
    Models sometimes wrap JSON with extra text. Try to recover.
    """
    s = s.strip()
    if not s:
        return None

    # If it's valid JSON already
    try:
        return json.loads(s)
    except Exception:
        pass

    # Try to find first {...} block
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return None
    blob = m.group(0)
    try:
        return json.loads(blob)
    except Exception:
        return None


# -----------------------------
# Qdrant reading / writing
# -----------------------------


def iter_points(qc: QdrantClient, collection: str, page_size: int = 256):
    """
    Scroll through Qdrant points (payload only).
    """
    offset = None
    while True:
        points, offset = qc.scroll(
            collection_name=collection,
            limit=page_size,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        if not points:
            break
        for p in points:
            yield p
        if offset is None:
            break


def load_checkpoint(path: Optional[str]) -> set[str]:
    done: set[str] = set()
    if not path:
        return done
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                sid = line.strip()
                if sid:
                    done.add(sid)
    return done


def append_checkpoint(path: Optional[str], song_id: str):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(song_id + "\n")


def get_point_ids_for_song(qc: QdrantClient, collection: str, song_id: str, page_size: int = 256) -> List[Any]:
    """
    Find ALL point IDs in Qdrant that belong to a given song_id (multiple chunks).
    Works with older client versions.
    """
    ids: List[Any] = []
    offset = None
    while True:
        points, offset = qc.scroll(
            collection_name=collection,
            limit=page_size,
            offset=offset,
            with_payload=False,
            with_vectors=False,
            scroll_filter={
                "must": [{"key": "song_id", "match": {"value": song_id}}],
            },
        )
        if not points:
            break
        for p in points:
            ids.append(p.id)
        if offset is None:
            break
    return ids


def upsert_payload_for_song(qc: QdrantClient, collection: str, song_id: str, payload_updates: Dict[str, Any]) -> int:
    """
    Update payload for all chunks belonging to song_id.
    Returns number of points updated.
    """
    point_ids = get_point_ids_for_song(qc, collection, song_id)
    if not point_ids:
        return 0

    # Older qdrant-client requires "points" positional arg
    qc.set_payload(
        collection_name=collection,
        payload=payload_updates,
        points=point_ids,
    )
    return len(point_ids)


# -----------------------------
# Ollama
# -----------------------------


SYSTEM_PROMPT = """You are a music metadata classifier for Tamil songs.
You will be given: title, movie, year, and a short lyrics excerpt (Tamil or Tanglish).
Return ONLY valid JSON with these keys exactly:
{
  "mood": "romantic|happy|sad|melancholic|kuthu|devotional|angry|inspirational|nostalgic|unknown",
  "genre": "melody|folk|gaana|kuthu|classical|devotional|romantic|dance|hiphop|rock|pop|unknown",
  "rhythm": "slow|mid|fast|unknown",
  "confidence": 0.0,
  "why": "short reason"
}
Rules:
- confidence must be between 0 and 1.
- If unsure, set unknown and low confidence.
- No extra keys, no markdown, no explanations outside JSON.
"""


def ollama_classify(
    ollama_url: str,
    model: str,
    title: str,
    movie: str,
    year: str,
    lyrics_excerpt: str,
    timeout_s: int,
    print_raw: bool,
) -> Tuple[Optional[dict], str]:
    """
    Returns (parsed_json, raw_text)
    """
    user_prompt = (
        f"TITLE: {title}\n"
        f"MOVIE: {movie}\n"
        f"YEAR: {year}\n"
        f"LYRICS_EXCERPT:\n{lyrics_excerpt}\n"
    )

    payload = {
        "model": model,
        "stream": False,
        # Many Ollama builds support format="json" for tighter output.
        # If your Ollama doesn't support it, it will still usually work with the strict system prompt.
        "format": "json",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        # Keep it short and deterministic
        "options": {
            "temperature": 0.1,
        },
    }

    r = requests.post(f"{ollama_url.rstrip('/')}/api/chat", json=payload, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()

    raw = ""
    # Ollama /api/chat returns {"message":{"content":"..."}}
    if isinstance(data, dict):
        raw = _safe_str((data.get("message") or {}).get("content"))
    raw = raw.strip()

    if print_raw:
        print("\n================================================================================")
        print("[LLM RAW OUTPUT]")
        print(raw)
        print("================================================================================\n")

    parsed = _extract_json_from_text(raw)
    return parsed, raw


def normalize_meta(meta: dict) -> dict:
    """
    Ensure schema + sanitize values
    """
    mood = _safe_str(meta.get("mood")).strip().lower()
    genre = _safe_str(meta.get("genre")).strip().lower()
    rhythm = _safe_str(meta.get("rhythm")).strip().lower()

    try:
        conf = float(meta.get("confidence", 0.0))
    except Exception:
        conf = 0.0
    conf = max(0.0, min(1.0, conf))

    why = _safe_str(meta.get("why")).strip()
    if len(why) > 200:
        why = why[:200].rstrip() + "…"

    return {
        "mood_llm": mood or "unknown",
        "genre_llm": genre or "unknown",
        "rhythm_llm": rhythm or "unknown",
        "meta_llm_confidence": conf,
        "meta_llm_why": why,
    }


# -----------------------------
# Main
# -----------------------------


def main():
    ap = argparse.ArgumentParser(description="Backfill mood/genre/rhythm using local Ollama model.")
    ap.add_argument("--qdrant-url", default="http://localhost:6333")
    ap.add_argument("--collection", required=True)
    ap.add_argument("--ollama-url", default="http://localhost:11434")
    ap.add_argument("--model", default="smollm2:135m")
    ap.add_argument("--page-size", type=int, default=256)
    ap.add_argument("--sleep-ms", type=int, default=0)
    ap.add_argument("--timeout-s", type=int, default=180, help="Ollama request timeout in seconds")
    ap.add_argument("--checkpoint", default="scripts/.checkpoint_song_meta.txt")
    ap.add_argument("--force", action="store_true", help="Recompute even if mood_llm/genre_llm/rhythm_llm already exist")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-every", type=int, default=25)
    ap.add_argument("--print-raw", action="store_true", help="Print raw LLM output for each processed song")
    ap.add_argument("--max-songs", type=int, default=None, help="Process only N songs (debug runs)")

    args = ap.parse_args()

    qc = QdrantClient(url=args.qdrant_url)

    done = load_checkpoint(args.checkpoint)

    # Collect unique songs (dedupe by song_id)
    unique: Dict[str, Dict[str, Any]] = {}
    for p in iter_points(qc, args.collection, page_size=args.page_size):
        payload = getattr(p, "payload", None) or {}
        sid = payload.get("song_id")
        if not sid:
            continue
        if sid in unique:
            continue
        unique[sid] = payload

    song_ids = list(unique.keys())
    if args.max_songs:
        song_ids = song_ids[: args.max_songs]

    print(f"Unique songs found: {len(unique)} (checkpointed: {len(done)})")
    total = len(song_ids)

    updated_songs = 0
    skipped = 0
    failed = 0

    for idx, sid in enumerate(song_ids, start=1):
        payload = unique[sid]

        title = _safe_str(payload.get("title"))
        movie = _safe_str(payload.get("movie"))
        year = _safe_str(payload.get("year"))

        lyrics_excerpt = pick_lyrics(payload, max_chars=500)

        if not lyrics_excerpt:
            skipped += 1
            if args.debug:
                print(f"[{idx}/{total}] SKIP no lyrics: {sid} {title}")
            append_checkpoint(args.checkpoint, sid)
            continue

        already = payload.get("mood_llm") and payload.get("genre_llm") and payload.get("rhythm_llm")
        if already and not args.force:
            skipped += 1
            if args.debug and (idx % args.debug_every == 0):
                print(f"[{idx}/{total}] SKIP already has llm meta: {sid}")
            append_checkpoint(args.checkpoint, sid)
            continue

        if args.debug and (idx % args.debug_every == 0 or args.debug_every == 1):
            print("\n" + "-" * 60)
            print(f"Song ID: {sid}")
            print(f"Title: {title} | Movie: {movie} | Year: {year}")
            print(f"Lyrics excerpt: {lyrics_excerpt}")
            print("-" * 60)

        try:
            meta, raw = ollama_classify(
                ollama_url=args.ollama_url,
                model=args.model,
                title=title,
                movie=movie,
                year=year,
                lyrics_excerpt=lyrics_excerpt,
                timeout_s=args.timeout_s,
                print_raw=args.print_raw,
            )

            if not meta:
                failed += 1
                # Still mark status in qdrant so we can detect failures later
                payload_updates = {
                    "meta_llm_status": "failed",
                    "meta_llm_updated_at": utc_now_iso(),
                    "meta_llm_source": f"ollama:{args.model}",
                    "meta_llm_error": "empty_or_unparseable_json",
                }
            else:
                norm = normalize_meta(meta)
                payload_updates = {
                    **norm,
                    "meta_llm_status": "ok",
                    "meta_llm_updated_at": utc_now_iso(),
                    "meta_llm_source": f"ollama:{args.model}",
                }

            if args.dry_run:
                # No write
                pass
            else:
                upsert_payload_for_song(qc, args.collection, sid, payload_updates)

            if payload_updates.get("meta_llm_status") == "ok":
                updated_songs += 1

        except requests.exceptions.ReadTimeout:
            failed += 1
            if args.debug:
                print(f"[{idx}/{total}] TIMEOUT on Ollama for song_id={sid}")
        except Exception as e:
            failed += 1
            if args.debug:
                print(f"[{idx}/{total}] ERROR song_id={sid}: {e}")

        append_checkpoint(args.checkpoint, sid)

        if args.sleep_ms and args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

        if idx % max(1, args.debug_every) == 0:
            print(f"[{idx}/{total}] updated_songs={updated_songs} skipped={skipped} failed={failed}")

    print("\nDONE")
    print(f"updated_songs={updated_songs} skipped={skipped} failed={failed}")
    print(f"checkpoint={args.checkpoint}")


if __name__ == "__main__":
    main()
