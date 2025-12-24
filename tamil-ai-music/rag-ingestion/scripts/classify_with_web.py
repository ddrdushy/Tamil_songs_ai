#!/usr/bin/env python3
"""
classify_with_web.py

Classify Tamil songs into mood / genre / rhythm using:
- lyrics excerpt (up to N chars)
- optional web search snippets (DuckDuckGo HTML)
- local Ollama model (qwen2.5:3b recommended)

Updates Qdrant payload for ALL chunks belonging to each song_id:
- mood_llm, genre_llm, rhythm_llm
- meta_source, meta_confidence, meta_updated_at
- optionally canonical fields mood/genre/rhythm if --write-canonical

Usage example:
python scripts/classify_with_web.py \
  --qdrant-url http://localhost:6333 \
  --collection songs_lyrics_v1 \
  --ollama-url http://localhost:11434 \
  --model qwen2.5:3b \
  --only-missing \
  --websearch \
  --print-raw --debug --debug-every 1 --max-songs 10 \
  --checkpoint .checkpoint_meta_run3.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


# ----------------------------
# Controlled label taxonomy
# ----------------------------
MOODS = [
    "romantic",
    "happy",
    "sad",
    "melancholic",
    "angry",
    "inspirational",
    "devotional",
    "kuthu",
    "celebration",
    "nostalgia",
    "unknown",
]

GENRES = [
    "romantic",
    "devotional",
    "folk",
    "kuthu",
    "melody",
    "dance",
    "classical",
    "hiphop",
    "rock",
    "love_duet",
    "sad",
    "unknown",
]

RHYTHMS = [
    "slow",
    "mid",
    "fast",
    "unknown",
]

BAD_VALUES = {None, "", "unknown", "na", "n/a", "none", "null"}


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _norm(s: Optional[str]) -> str:
    if s is None:
        return "unknown"
    s = str(s).strip().lower()
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s or "unknown"


def _canonicalize(label: str, allowed: List[str]) -> str:
    x = _norm(label)
    # common aliases
    alias = {
        "melodic": "melody",
        "melodious": "melody",
        "sentimental": "melancholic",
        "party": "celebration",
        "uplifting": "inspirational",
        "spiritual": "devotional",
        "duet": "love_duet",
        "love": "romantic",
        "beat": "fast",
        "medium": "mid",
        "moderate": "mid",
    }
    x = alias.get(x, x)
    return x if x in allowed else "unknown"


def _is_missing(v: Optional[str]) -> bool:
    return _norm(v) in BAD_VALUES


def _strip_noise(text: str) -> str:
    """Remove common credit lines in lyrics to reduce confusion."""
    if not text:
        return ""
    t = text
    # remove common headers (Tamil + English patterns)
    t = re.sub(r"(singers?\s*:.*)", "", t, flags=re.I)
    t = re.sub(r"(music\s+director\s*:.*)", "", t, flags=re.I)
    t = re.sub(r"(lyricist\s*:.*)", "", t, flags=re.I)
    t = re.sub(r"(பாடகர்\s*:.*)", "", t)
    t = re.sub(r"(பாடகி\s*:.*)", "", t)
    t = re.sub(r"(இசை\s+அமைப்பாளர்\s*:.*)", "", t)
    t = re.sub(r"(பாடலாசிரியர்\s*:.*)", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ----------------------------
# Web search (DuckDuckGo HTML)
# ----------------------------
def _ddg_search_snippets(query: str, k: int = 3, timeout_s: int = 20, sleep_ms: int = 200) -> List[str]:
    """
    Lightweight web search without API keys.
    Uses DuckDuckGo HTML endpoint and extracts result snippets.
    """
    if not query.strip():
        return []

    url = "https://duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (TamilMusicAI/1.0; +https://example.com)",
    }
    try:
        time.sleep(max(0, sleep_ms) / 1000.0)
        r = requests.post(url, data={"q": query}, headers=headers, timeout=timeout_s)
        if r.status_code != 200:
            return []
        html = r.text

        # Extract snippets; DDG uses <a class="result__snippet"> or <div class="result__snippet">
        snippets = re.findall(r'result__snippet[^>]*>(.*?)<', html)
        clean: List[str] = []
        for sn in snippets:
            sn = re.sub(r"<.*?>", "", sn)
            sn = re.sub(r"&nbsp;|&amp;|&quot;|&#39;", " ", sn)
            sn = re.sub(r"\s+", " ", sn).strip()
            if sn and sn not in clean:
                clean.append(sn)
            if len(clean) >= k:
                break
        return clean[:k]
    except Exception:
        return []


# ----------------------------
# Ollama call + robust JSON parse
# ----------------------------
def _extract_json_block(text: str) -> Optional[dict]:
    """Try to locate a JSON object inside text."""
    if not text:
        return None
    text = text.strip()

    # direct parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # find first {...} block
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        block = m.group(0)
        try:
            return json.loads(block)
        except Exception:
            return None
    return None


def _ollama_classify(
    ollama_url: str,
    model: str,
    title: str,
    movie: Optional[str],
    year: Optional[str],
    lyrics_excerpt: str,
    web_snippets: List[str],
    timeout_s: int = 180,
    debug: bool = False,
) -> Tuple[Optional[dict], str]:
    """
    Returns (parsed_json, raw_text)
    """
    system = (
        "You are a music metadata classifier for Tamil songs.\n"
        "You MUST follow the taxonomy exactly.\n\n"
        f"Allowed moods: {MOODS}\n"
        f"Allowed genres: {GENRES}\n"
        f"Allowed rhythms: {RHYTHMS}\n\n"
        "Rules:\n"
        "- Choose EXACTLY ONE value for mood, genre, rhythm.\n"
        "- If unsure, output 'unknown'.\n"
        "- confidence must be a float 0.0..1.0\n"
        "- Output JSON ONLY. No markdown, no extra commentary.\n"
    )

    web_context = ""
    if web_snippets:
        # Keep it small so it doesn't overwhelm lyrics
        joined = " | ".join(web_snippets[:3])
        web_context = f"\nWeb snippets (may help): {joined}\n"

    user = (
        f"Song title: {title}\n"
        f"Movie: {movie or 'unknown'}\n"
        f"Year: {year or 'unknown'}\n"
        f"{web_context}\n"
        "Lyrics excerpt:\n"
        f"{lyrics_excerpt}\n\n"
        "Return JSON with keys: mood, genre, rhythm, confidence, why\n"
        "where mood/genre/rhythm must be from the allowed lists.\n"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        # Ollama supports forcing JSON for many models:
        "format": "json",
        "options": {
            "temperature": 0.1,
            "num_predict": 180,
        },
    }

    r = requests.post(f"{ollama_url.rstrip('/')}/api/chat", json=payload, timeout=timeout_s)
    r.raise_for_status()
    j = r.json()
    raw = (j.get("message") or {}).get("content") or ""

    parsed = _extract_json_block(raw)
    if debug and not parsed:
        # sometimes model returns empty; keep raw for visibility
        return None, raw

    return parsed, raw


# ----------------------------
# Qdrant helpers
# ----------------------------
def _scroll_unique_songs(
    client: QdrantClient,
    collection: str,
    page_size: int,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Returns a list of song objects:
    {song_id, title, movie, year, lyrics_excerpt}
    One per unique song_id.
    """
    seen = {}
    offset = None
    fetched_points = 0

    while True:
        points, offset = client.scroll(
            collection_name=collection,
            scroll_filter=None,
            with_payload=True,
            with_vectors=False,
            limit=page_size,
            offset=offset,
        )
        if not points:
            break

        for p in points:
            fetched_points += 1
            payload = p.payload or {}
            sid = payload.get("song_id")
            if not sid or sid in seen:
                continue

            title = payload.get("title") or ""
            movie = payload.get("movie")
            year = payload.get("year")
            # pick lyrics text field
            txt = payload.get("chunk_text") or payload.get("lyrics_text") or ""
            txt = _strip_noise(str(txt))
            excerpt = txt[:500]  # you said 500 chars is fine

            # some records might not have lyrics in this chunk; still store; we might find later chunks via ids fetch
            seen[sid] = {
                "song_id": sid,
                "title": title,
                "movie": movie,
                "year": str(year) if year is not None else None,
                "lyrics_excerpt": excerpt,
            }

            if limit and len(seen) >= limit:
                return list(seen.values())

        if offset is None:
            break

    return list(seen.values())


def _get_point_ids_for_song(client: QdrantClient, collection: str, song_id: str) -> List[str]:
    """Collect ALL point IDs where payload.song_id == song_id."""
    filt = rest.Filter(
        must=[rest.FieldCondition(key="song_id", match=rest.MatchValue(value=song_id))]
    )

    ids: List[str] = []
    offset = None
    while True:
        points, offset = client.scroll(
            collection_name=collection,
            scroll_filter=filt,
            with_payload=False,
            with_vectors=False,
            limit=256,
            offset=offset,
        )
        if not points:
            break
        for p in points:
            # Qdrant point id can be int or str; store as str
            ids.append(str(p.id))
        if offset is None:
            break
    return ids


def _set_payload_for_song(client: QdrantClient, collection: str, song_id: str, payload_updates: Dict[str, Any]) -> int:
    """Update payload on all points for a song."""
    ids = _get_point_ids_for_song(client, collection, song_id)
    if not ids:
        return 0
    client.set_payload(collection_name=collection, payload=payload_updates, points=ids)
    return len(ids)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--qdrant-url", default="http://localhost:6333")
    ap.add_argument("--collection", required=True)

    ap.add_argument("--ollama-url", default="http://localhost:11434")
    ap.add_argument("--model", default="qwen2.5:3b")

    ap.add_argument("--page-size", type=int, default=512)
    ap.add_argument("--max-songs", type=int, default=0, help="Process only N songs (0 = all)")

    ap.add_argument("--only-missing", action="store_true", help="Only classify when llm fields are missing/unknown")
    ap.add_argument("--write-canonical", action="store_true", help="Also write mood/genre/rhythm canonical fields")

    ap.add_argument("--websearch", action="store_true", help="Enable web search snippets for the LLM")
    ap.add_argument("--web-k", type=int, default=3)
    ap.add_argument("--web-timeout", type=int, default=20)
    ap.add_argument("--web-sleep-ms", type=int, default=250)

    ap.add_argument("--ollama-timeout", type=int, default=180)

    ap.add_argument("--checkpoint", default=".checkpoint_meta.jsonl")
    ap.add_argument("--print-raw", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-every", type=int, default=50)

    args = ap.parse_args()

    qdrant = QdrantClient(url=args.qdrant_url)

    # load checkpointed song_ids
    done = set()
    if os.path.exists(args.checkpoint):
        with open(args.checkpoint, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    sid = rec.get("song_id")
                    if sid:
                        done.add(sid)
                except Exception:
                    continue

    limit = args.max_songs if args.max_songs and args.max_songs > 0 else None

    songs = _scroll_unique_songs(qdrant, args.collection, args.page_size, limit=None)
    print(f"Unique songs found: {len(songs)} (checkpointed: {len(done)})")

    updated_meta_songs = 0
    skipped = 0
    failed = 0

    to_process = songs
    if limit:
        to_process = songs[:limit]

    for idx, s in enumerate(to_process, start=1):
        sid = s["song_id"]
        if sid in done:
            skipped += 1
            continue

        # fetch any existing llm fields from ANY chunk (we’ll read from first available chunk via scroll filter)
        # pick one point payload
        filt = rest.Filter(must=[rest.FieldCondition(key="song_id", match=rest.MatchValue(value=sid))])
        pts, _ = qdrant.scroll(
            collection_name=args.collection,
            scroll_filter=filt,
            with_payload=True,
            with_vectors=False,
            limit=1,
        )
        payload0 = (pts[0].payload or {}) if pts else {}
        mood_llm_existing = payload0.get("mood_llm")
        genre_llm_existing = payload0.get("genre_llm")
        rhythm_llm_existing = payload0.get("rhythm_llm")

        if args.only_missing:
            if (not _is_missing(mood_llm_existing)) and (not _is_missing(genre_llm_existing)) and (not _is_missing(rhythm_llm_existing)):
                skipped += 1
                with open(args.checkpoint, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"song_id": sid, "status": "skipped_already_filled", "ts": _now_iso()}) + "\n")
                continue

        title = s.get("title") or payload0.get("title") or ""
        movie = s.get("movie") or payload0.get("movie")
        year = s.get("year") or payload0.get("year")
        year = str(year) if year is not None else None

        lyrics_excerpt = s.get("lyrics_excerpt") or _strip_noise(str(payload0.get("chunk_text") or payload0.get("lyrics_text") or ""))[:500]

        if not lyrics_excerpt or len(lyrics_excerpt.strip()) < 40:
            skipped += 1
            with open(args.checkpoint, "a", encoding="utf-8") as f:
                f.write(json.dumps({"song_id": sid, "status": "skipped_no_lyrics", "ts": _now_iso()}) + "\n")
            continue

        if args.debug and (idx % max(1, args.debug_every) == 0 or args.debug_every == 1):
            print("\n" + "-" * 60)
            print(f"Song ID: {sid}")
            print(f"Title: {title} | Movie: {movie} | Year: {year}")
            print(f"Lyrics excerpt: {lyrics_excerpt[:220]}")
            print()

        web_snips: List[str] = []
        if args.websearch:
            q = f'{title} {movie or ""} {year or ""} Tamil song mood genre'
            web_snips = _ddg_search_snippets(q, k=args.web_k, timeout_s=args.web_timeout, sleep_ms=args.web_sleep_ms)

        try:
            parsed, raw = _ollama_classify(
                ollama_url=args.ollama_url,
                model=args.model,
                title=title,
                movie=movie,
                year=year,
                lyrics_excerpt=lyrics_excerpt,
                web_snippets=web_snips,
                timeout_s=args.ollama_timeout,
                debug=args.debug,
            )

            if args.print_raw:
                print("=" * 80)
                print("[LLM RAW OUTPUT]")
                print(raw.strip())
                print("=" * 80)

            if not parsed:
                failed += 1
                with open(args.checkpoint, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"song_id": sid, "status": "llm_failed", "ts": _now_iso()}) + "\n")
                continue

            mood = _canonicalize(parsed.get("mood"), MOODS)
            genre = _canonicalize(parsed.get("genre"), GENRES)
            rhythm = _canonicalize(parsed.get("rhythm"), RHYTHMS)
            confidence = parsed.get("confidence")
            try:
                confidence = float(confidence)
            except Exception:
                confidence = 0.3
            confidence = max(0.0, min(1.0, confidence))

            why = parsed.get("why")
            if why is not None:
                why = str(why)[:280]

            payload_updates: Dict[str, Any] = {
                "mood_llm": mood,
                "genre_llm": genre,
                "rhythm_llm": rhythm,
                "meta_source": f"ollama:{args.model}" + (":web" if args.websearch else ""),
                "meta_confidence": confidence,
                "meta_updated_at": _now_iso(),
            }
            if why:
                payload_updates["meta_why"] = why

            if args.write_canonical:
                # Only overwrite canonical if missing/unknown currently
                if _is_missing(payload0.get("mood")):
                    payload_updates["mood"] = mood
                if _is_missing(payload0.get("genre")):
                    payload_updates["genre"] = genre
                if _is_missing(payload0.get("rhythm")):
                    payload_updates["rhythm"] = rhythm

            # update all chunks for that song
            _set_payload_for_song(qdrant, args.collection, sid, payload_updates)

            updated_meta_songs += 1
            with open(args.checkpoint, "a", encoding="utf-8") as f:
                f.write(json.dumps({"song_id": sid, "status": "updated", **payload_updates}) + "\n")

        except Exception as e:
            failed += 1
            with open(args.checkpoint, "a", encoding="utf-8") as f:
                f.write(json.dumps({"song_id": sid, "status": "llm_failed", "error": str(e)[:300], "ts": _now_iso()}) + "\n")

        if args.debug and (idx % max(1, args.debug_every) == 0 or args.debug_every == 1):
            print(f"[{idx}/{len(to_process)}] updated_meta_songs={updated_meta_songs} skipped={skipped} failed={failed}")

    print("\nDone.")
    print(f"Updated meta songs: {updated_meta_songs}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Checkpoint: {args.checkpoint}")


if __name__ == "__main__":
    main()
