#!/usr/bin/env python3
"""
classify_with_web.py

Classify Tamil songs into mood/genre/rhythm using:
- Local Ollama model (qwen2.5:3b recommended)
- Lyrics excerpt (Tamil / Tanglish)
- Optional web snippets (DuckDuckGo HTML search)

Then upsert payload into Qdrant for ALL chunks of each song_id.
Compatible with older qdrant-client where set_payload requires `points` (IDs)
and does not accept filter/points_selector kwargs.

Usage examples:

# Update only missing (unknown/empty) meta fields
python scripts/classify_with_web.py \
  --qdrant-url http://localhost:6333 \
  --collection songs_lyrics_v1 \
  --ollama-url http://localhost:11434 \
  --model qwen2.5:3b \
  --only-missing \
  --debug-every 1 \
  --print-raw

# Force update ALL songs (rebuild meta)
python scripts/classify_with_web.py \
  --qdrant-url http://localhost:6333 \
  --collection songs_lyrics_v1 \
  --ollama-url http://localhost:11434 \
  --model qwen2.5:3b \
  --force \
  --checkpoint .checkpoint_meta_force.jsonl

# Fix bad youtube_url records too (search-query URLs)
python scripts/classify_with_web.py \
  --qdrant-url http://localhost:6333 \
  --collection songs_lyrics_v1 \
  --ollama-url http://localhost:11434 \
  --model qwen2.5:3b \
  --only-missing \
  --fix-bad-youtube \
  --checkpoint .checkpoint_meta_run2.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from qdrant_client import QdrantClient


# ---------------------------
# Helpers
# ---------------------------

def now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def clamp_text(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1].rstrip() + "…"


def load_checkpoint(path: str) -> set[str]:
    if not path or not os.path.exists(path):
        return set()
    done = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                sid = obj.get("song_id")
                if sid:
                    done.add(sid)
            except Exception:
                continue
    return done


def append_checkpoint(path: str, rec: dict) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------
# Web search (DuckDuckGo HTML)
# ---------------------------

DUCK_URL = "https://html.duckduckgo.com/html/"


def ddg_search(query: str, top_k: int = 3, timeout_s: int = 15) -> List[Dict[str, str]]:
    """
    Very lightweight web 'search' without API keys.
    Returns list of {title, url, snippet}.
    """
    query = (query or "").strip()
    if not query:
        return []

    headers = {
        "User-Agent": "TamilMusicAI/1.0 (+local-script)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        r = requests.post(
            DUCK_URL,
            data={"q": query},
            headers=headers,
            timeout=timeout_s,
        )
        r.raise_for_status()
        html = r.text
    except Exception:
        return []

    # Basic parsing via regex (good enough for snippets)
    results = []
    # Each result block typically contains: result__a (title/url) and result__snippet
    link_pat = re.compile(r'<a rel="nofollow" class="result__a" href="([^"]+)"[^>]*>(.*?)</a>', re.S)
    snip_pat = re.compile(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', re.S)

    links = link_pat.findall(html)
    snippets = snip_pat.findall(html)

    def strip_tags(x: str) -> str:
        x = re.sub(r"<[^>]+>", " ", x)
        x = re.sub(r"\s+", " ", x).strip()
        return x

    for i, (url, title_html) in enumerate(links[:top_k]):
        title = strip_tags(title_html)
        snip = strip_tags(snippets[i]) if i < len(snippets) else ""
        results.append({"title": title, "url": url, "snippet": snip})

    return results


# ---------------------------
# Ollama classify
# ---------------------------

ALLOWED_MOODS = ["romantic", "happy", "sad", "melancholic", "kuthu", "devotional", "angry", "inspirational", "unknown"]
ALLOWED_GENRES = ["love", "dance", "celebration", "heartbreak", "friendship", "devotion", "nostalgia", "folk", "melody", "unknown"]
ALLOWED_RHYTHMS = ["fast", "medium", "slow", "unknown"]


def build_prompt(title: str, movie: str, year: str, lyrics_excerpt: str, web_snips: List[Dict[str, str]]) -> str:
    """
    Prompt designed to reduce random wrong labels.
    Enforces fixed enums + JSON-only output.
    """
    web_block = ""
    if web_snips:
        lines = []
        for i, it in enumerate(web_snips, 1):
            lines.append(f"{i}. {it.get('title','')}\n   {it.get('snippet','')}\n   {it.get('url','')}")
        web_block = "\n\nWEB SNIPPETS (may be noisy):\n" + "\n".join(lines)

    return f"""
You are a music metadata classifier for Tamil songs.

TASK:
Given the song info and lyrics excerpt, classify:
- mood: one of {ALLOWED_MOODS}
- genre: one of {ALLOWED_GENRES}
- rhythm: one of {ALLOWED_RHYTHMS}

IMPORTANT RULES:
1) Only output valid JSON (no markdown, no commentary).
2) Choose exactly ONE label for each field.
3) If evidence is insufficient or unclear, output "unknown".
4) Prefer lyrics meaning + tone. Do NOT guess devotional/folk unless strongly indicated.
5) rhythm:
   - "fast" for energetic/dance/kuthu/party feel
   - "slow" for emotional/ballad/soft devotional
   - "medium" otherwise

Return JSON with keys:
{{
  "mood": "...",
  "genre": "...",
  "rhythm": "...",
  "confidence": 0.0-1.0,
  "why": "one short sentence"
}}

SONG:
Title: {title}
Movie: {movie}
Year: {year}

LYRICS EXCERPT (may include Tamil/Tanglish):
{lyrics_excerpt}
{web_block}
""".strip()


def ollama_chat(
    ollama_url: str,
    model: str,
    prompt: str,
    timeout_s: int = 180,
) -> Tuple[str, Optional[dict]]:
    """
    Returns (raw_text, parsed_json_or_none)
    """
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You output ONLY strict JSON as requested."},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        # Some models behave better with lower temperature for classification:
        "options": {"temperature": 0.2},
    }

    r = requests.post(f"{ollama_url.rstrip('/')}/api/chat", json=payload, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()

    content = (
        data.get("message", {}).get("content")
        or data.get("response")
        or ""
    ).strip()

    # Extract first JSON object if model adds junk
    parsed = None
    if content:
        m = re.search(r"\{.*\}", content, re.S)
        if m:
            candidate = m.group(0)
            try:
                parsed = json.loads(candidate)
            except Exception:
                parsed = None
    return content, parsed


def normalize_meta(meta: dict) -> Optional[dict]:
    if not meta:
        return None

    mood = safe_str(meta.get("mood")).strip().lower()
    genre = safe_str(meta.get("genre")).strip().lower()
    rhythm = safe_str(meta.get("rhythm")).strip().lower()

    if mood not in ALLOWED_MOODS:
        mood = "unknown"
    if genre not in ALLOWED_GENRES:
        genre = "unknown"
    if rhythm not in ALLOWED_RHYTHMS:
        rhythm = "unknown"

    try:
        conf = float(meta.get("confidence", 0.0))
    except Exception:
        conf = 0.0
    conf = max(0.0, min(1.0, conf))

    why = clamp_text(safe_str(meta.get("why")), 200)

    return {"mood": mood, "genre": genre, "rhythm": rhythm, "confidence": conf, "why": why}


# ---------------------------
# Qdrant helpers
# ---------------------------

def is_missing(payload: dict) -> bool:
    """
    'only-missing' = any of the llm fields are missing/unknown/empty.
    """
    def bad(v: Any) -> bool:
        s = safe_str(v).strip().lower()
        return (not s) or (s == "unknown")

    return bad(payload.get("mood_llm")) or bad(payload.get("genre_llm")) or bad(payload.get("rhythm_llm"))


def is_bad_youtube_url(payload: dict) -> bool:
    """
    Old runs stored youtube 'search results' URLs instead of watch URLs.
    We mark them as bad to re-fix later.
    """
    url = safe_str(payload.get("youtube_url")).strip()
    if not url:
        return False
    return "youtube.com/results?search_query=" in url


def get_unique_songs(client: QdrantClient, collection: str, page_size: int = 256, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Scroll all points, dedupe by song_id, keep minimal fields for processing.
    NOTE: We'll also keep the first chunk_text as lyrics excerpt source.
    """
    songs: Dict[str, Dict[str, Any]] = {}
    offset = None
    total_seen = 0

    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=page_size,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        if not points:
            break

        for p in points:
            total_seen += 1
            payload = p.payload or {}
            sid = payload.get("song_id")
            if not sid:
                continue

            if sid not in songs:
                songs[sid] = {
                    "song_id": sid,
                    "title": payload.get("title"),
                    "movie": payload.get("movie"),
                    "year": payload.get("year"),
                    "song_url": payload.get("song_url"),
                    "chunk_text": payload.get("chunk_text") or payload.get("lyrics_text") or payload.get("text") or "",
                    "payload_sample": payload,  # for only-missing checks etc
                }

        if limit and len(songs) >= limit:
            break

        if next_offset is None:
            break
        offset = next_offset

    return list(songs.values())


def get_point_ids_for_song(client: QdrantClient, collection: str, song_id: str, page_size: int = 256) -> List[Any]:
    """
    Collect ALL point IDs that belong to this song_id, by scrolling.
    Compatible with older qdrant-client: we scroll + filter, then set_payload(points=[ids...])
    """
    ids = []
    offset = None

    # Qdrant filter format as dict works across many versions
    flt = {"must": [{"key": "song_id", "match": {"value": song_id}}]}

    while True:
        pts, next_offset = client.scroll(
            collection_name=collection,
            scroll_filter=flt,
            limit=page_size,
            offset=offset,
            with_payload=False,
            with_vectors=False,
        )
        if not pts:
            break
        for p in pts:
            ids.append(p.id)
        if next_offset is None:
            break
        offset = next_offset

    return ids


def upsert_payload_all_chunks(client: QdrantClient, collection: str, song_id: str, payload_updates: dict, page_size: int = 256) -> int:
    """
    Update payload for ALL chunks (points) belonging to song_id.
    Returns number of points updated.
    """
    point_ids = get_point_ids_for_song(client, collection, song_id, page_size=page_size)
    if not point_ids:
        return 0

    # Older clients require `points` positional/kw arg; no filter support.
    client.set_payload(
        collection_name=collection,
        points=point_ids,
        payload=payload_updates,
    )
    return len(point_ids)


# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qdrant-url", default="http://localhost:6333")
    ap.add_argument("--collection", required=True)
    ap.add_argument("--ollama-url", default="http://localhost:11434")
    ap.add_argument("--model", default="qwen2.5:3b")

    ap.add_argument("--page-size", type=int, default=256)
    ap.add_argument("--limit", type=int, default=None, help="Limit unique songs (for testing).")
    ap.add_argument("--max-songs", type=int, default=None, help="Stop after N processed songs (debug).")

    ap.add_argument("--sleep-ms", type=int, default=0, help="Sleep between songs (avoid overheating).")
    ap.add_argument("--timeout-s", type=int, default=180)

    ap.add_argument("--only-missing", action="store_true", help="Only classify if mood_llm/genre_llm/rhythm_llm missing or unknown.")
    ap.add_argument("--force", action="store_true", help="Force re-classify even if fields exist.")
    ap.add_argument("--write-canonical", action="store_true", help="Also write canonical fields mood/genre/rhythm (optional).")
    ap.add_argument("--dry-run", action="store_true")

    ap.add_argument("--fix-bad-youtube", action="store_true", help="Mark bad youtube_url (search-results URLs) for re-fix later.")

    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-every", type=int, default=50)
    ap.add_argument("--print-raw", action="store_true")

    ap.add_argument("--checkpoint", default=".checkpoint_meta.jsonl")

    args = ap.parse_args()

    if args.force and args.only_missing:
        print("NOTE: --force overrides --only-missing")
        args.only_missing = False

    done = load_checkpoint(args.checkpoint)
    client = QdrantClient(url=args.qdrant_url)

    songs = get_unique_songs(client, args.collection, page_size=args.page_size, limit=args.limit)
    total = len(songs)

    print(f"Loaded checkpointed songs: {len(done)}")
    print(f"Unique songs found: {total} (checkpointed: {len(done)})")

    updated_meta = 0
    skipped = 0
    failed = 0
    youtube_fixed = 0

    t0 = time.time()
    processed = 0

    for idx, s in enumerate(songs, 1):
        sid = s["song_id"]
        if sid in done:
            skipped += 1
            continue

        payload_sample = s.get("payload_sample") or {}
        title = safe_str(s.get("title"))
        movie = safe_str(s.get("movie"))
        year = safe_str(s.get("year"))
        song_url = safe_str(s.get("song_url"))
        lyrics_excerpt = clamp_text(safe_str(s.get("chunk_text")), 600)

        # If no lyrics text in Qdrant payload, skip
        if not lyrics_excerpt.strip():
            append_checkpoint(args.checkpoint, {"song_id": sid, "status": "skipped_no_lyrics", "ts": now_iso()})
            skipped += 1
            continue

        # only-missing logic
        if args.only_missing and (not is_missing(payload_sample)):
            append_checkpoint(args.checkpoint, {"song_id": sid, "status": "skipped_has_meta", "ts": now_iso()})
            skipped += 1
            continue

        # optional: mark bad youtube_url
        if args.fix_bad_youtube and is_bad_youtube_url(payload_sample):
            if not args.dry_run:
                try:
                    upsert_payload_all_chunks(
                        client,
                        args.collection,
                        sid,
                        {
                            "youtube_url": None,
                            "youtube_status": "needs_refetch",
                            "youtube_updated_at": now_iso(),
                        },
                        page_size=args.page_size,
                    )
                    youtube_fixed += 1
                except Exception as e:
                    # Don't fail the entire meta classify for this
                    if args.debug:
                        print(f"[WARN] youtube fix failed for {sid}: {e}")

        # Web search query: title + movie + year
        query = " ".join([x for x in [title.replace("Song Lyrics", "").strip(), movie, year, "Tamil song"] if x]).strip()
        web_snips = ddg_search(query, top_k=3, timeout_s=15)

        if args.debug and (idx % max(1, args.debug_every) == 0):
            print("\n" + "-" * 60)
            print(f"[{idx}/{total}] Song ID: {sid}")
            print(f"Title: {title} | Movie: {movie} | Year: {year}")
            print(f"Lyrics excerpt: {lyrics_excerpt[:180]}...")

        prompt = build_prompt(title=title, movie=movie, year=year, lyrics_excerpt=lyrics_excerpt, web_snips=web_snips)

        try:
            raw, parsed = ollama_chat(
                ollama_url=args.ollama_url,
                model=args.model,
                prompt=prompt,
                timeout_s=args.timeout_s,
            )

            if args.print_raw:
                print("\n" + "=" * 80)
                print("[LLM RAW OUTPUT]")
                print(raw)
                print("=" * 80 + "\n")

            meta = normalize_meta(parsed or {})
            if not meta:
                append_checkpoint(args.checkpoint, {"song_id": sid, "status": "failed_parse", "ts": now_iso(), "raw": clamp_text(raw, 500)})
                failed += 1
                continue

            payload_updates = {
                "mood_llm": meta["mood"],
                "genre_llm": meta["genre"],
                "rhythm_llm": meta["rhythm"],
                "meta_confidence": meta["confidence"],
                "meta_source": f"ollama:{args.model}+ddg",
                "meta_updated_at": now_iso(),
                "meta_why": meta["why"],
            }

            # Optional: overwrite canonical fields too (if you want the UI to read mood directly)
            if args.write_canonical:
                payload_updates.update({
                    "mood": meta["mood"],
                    "genre": meta["genre"],
                    "rhythm": meta["rhythm"],
                })

            if not args.dry_run:
                pts_updated = upsert_payload_all_chunks(
                    client,
                    args.collection,
                    sid,
                    payload_updates,
                    page_size=args.page_size,
                )

            updated_meta += 1
            append_checkpoint(args.checkpoint, {"song_id": sid, "status": "updated", "ts": now_iso(), **payload_updates})

        except requests.exceptions.Timeout:
            append_checkpoint(args.checkpoint, {"song_id": sid, "status": "failed_timeout", "ts": now_iso()})
            failed += 1
        except Exception as e:
            append_checkpoint(args.checkpoint, {"song_id": sid, "status": "failed_exception", "ts": now_iso(), "err": safe_str(e)})
            failed += 1

        processed += 1
        # Progress log every song (simple + clear)
        elapsed = time.time() - t0
        rate = processed / elapsed if elapsed > 0 else 0.0
        eta = (total - idx) / rate if rate > 0 else 0.0
        print(f"[{idx}/{total}] updated_meta={updated_meta} skipped={skipped} failed={failed} youtube_fixed={youtube_fixed} | {rate:.2f}/s ETA~{eta/60:.1f}m")

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

        if args.max_songs and processed >= args.max_songs:
            break

    print("\nDone.")
    print(f"Updated meta songs: {updated_meta}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"YouTube fixed: {youtube_fixed}")
    print(f"Elapsed: {time.time() - t0:.1f}s")
    print(f"Checkpoint: {args.checkpoint}")


if __name__ == "__main__":
    main()
