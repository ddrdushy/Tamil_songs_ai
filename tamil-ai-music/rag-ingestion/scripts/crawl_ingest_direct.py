"""
Direct pipeline (no saved enriched dataset):
1) Crawl tamil2lyrics (append-only raw temp file)
2) For each crawled song: classify mood/decade/themes (embedding-based)
3) Chunk + embed lyrics and upsert directly into Qdrant
4) Use state DB to skip unchanged songs (lyrics_hash)
"""

from __future__ import annotations

import json
import hashlib
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer

from src.config import QDRANT_URL, COLLECTION, EMBED_MODEL
from src.preprocess import chunk_text
from src.state_store import StateStore

# Use your existing crawler + embedding classifier
from scripts import crawl as crawler
from scripts.enrich import classify_with_embeddings, derive_decade

# Deterministic point IDs
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def stable_song_id(song_url: str) -> str:
    return hashlib.sha1(song_url.encode("utf-8")).hexdigest()


def sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def make_point_id(song_id: str, chunk_idx: int) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{song_id}:{chunk_idx}"))


def delete_song_chunks(client: QdrantClient, song_id: str):
    client.delete(
        collection_name=COLLECTION,
        points_selector=Filter(
            must=[FieldCondition(key="song_id", match=MatchValue(value=song_id))]
        ),
    )


def enrich_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    # Build "lyrics" field for embedding + hashing
    lyrics_translit = rec.get("english_lyrics") or rec.get("lyrics_translit") or ""
    lyrics_ta = rec.get("tamil_lyrics") or rec.get("lyrics_ta") or ""
    lyrics = (lyrics_ta or "").strip() + "\n" + (lyrics_translit or "").strip()
    lyrics = lyrics.strip()

    # Embedding-based classifier
    pm, energy, themes, ff = classify_with_embeddings(lyrics_translit, lyrics_ta)
    decade = derive_decade(rec.get("movie_year"))

    # Stable IDs + hashes for incremental updates
    song_url = rec.get("song_url") or ""
    sid = rec.get("song_id") or (stable_song_id(song_url) if song_url else sha1(lyrics)[:16])

    # lyrics_hash changes only when lyrics content changes
    lyrics_hash = sha1(lyrics)

    # meta_hash changes when metadata changes (mood/decade/themes/title/etc.)
    meta_obj = {
        "primary_mood": pm,
        "energy_level": energy,
        "theme_tags": themes,
        "is_family_friendly": ff,
        "decade": decade,
        "movie_title": rec.get("movie_title"),
        "movie_year": rec.get("movie_year"),
        "song_title": rec.get("song_title"),
        "singer": rec.get("singer"),
        "music_by": rec.get("music_by"),
        "song_url": song_url,
    }
    meta_hash = sha1(json.dumps(meta_obj, ensure_ascii=False, sort_keys=True))

    rec["song_id"] = sid
    rec["lyrics"] = lyrics
    rec["primary_mood"] = pm
    rec["energy_level"] = energy
    rec["theme_tags"] = themes
    rec["is_family_friendly"] = ff
    rec["decade"] = decade
    rec["lyrics_hash"] = lyrics_hash
    rec["meta_hash"] = meta_hash
    return rec


def ingest_record(
    client: QdrantClient,
    model: SentenceTransformer,
    state: StateStore,
    rec: Dict[str, Any],
) -> int:
    sid = rec["song_id"]
    prev = state.get(sid)
    is_new = prev is None
    is_changed = (prev is not None and prev[0] != rec["lyrics_hash"])

    if not (is_new or is_changed):
        return 0

    if is_changed:
        delete_song_chunks(client, sid)

    chunks = chunk_text(rec["lyrics"], chunk_size=1200, overlap=200)
    if not chunks:
        # still record state so we don’t retry forever
        state.upsert(sid, rec["lyrics_hash"], rec["meta_hash"])
        return 0

    vectors = model.encode(
        chunks,
        batch_size=32,
        show_progress_bar=False,
        normalize_embeddings=True,
    )

    points: List[PointStruct] = []
    for i, (chunk, vec) in enumerate(zip(chunks, vectors)):
        payload: Dict[str, Any] = {
            "song_id": sid,
            "chunk_id": i,
            "mood": rec.get("primary_mood"),
            "decade": rec.get("decade"),
            "title": rec.get("song_title"),
            "singer": rec.get("singer"),
            "movie": rec.get("movie_title"),
            "year": rec.get("movie_year"),
            "themes": rec.get("theme_tags"),
            "song_url": rec.get("song_url"),
            "chunk_text": chunk,
        }
        points.append(
            PointStruct(
                id=make_point_id(sid, i),
                vector=vec.tolist(),
                payload=payload,
            )
        )

    client.upsert(collection_name=COLLECTION, points=points)
    state.upsert(sid, rec["lyrics_hash"], rec["meta_hash"])
    return len(points)


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main(
    max_pages: Optional[int] = None,
    raw_temp: Path = Path("data/raw/fresh_raw.jsonl"),
):
    raw_temp.parent.mkdir(parents=True, exist_ok=True)

    print("=== 1) Crawl to temp raw file ===")
    crawler.scrape_all_json(output_file=str(raw_temp), max_pages=max_pages)

    print("=== 2) Direct enrich + ingest ===")
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBED_MODEL)
    state = StateStore()
    print("Using state DB:", state.path)

    scanned = 0
    updated_songs = 0
    upserted_points = 0

    for rec in iter_jsonl(raw_temp):
        scanned += 1
        rec = enrich_record(rec)

        pts = ingest_record(client, model, state, rec)
        if pts > 0:
            updated_songs += 1
            upserted_points += pts

        # Lightweight progress every 200 songs
        if scanned % 200 == 0:
            print(f"[PROGRESS] scanned={scanned} updated_songs={updated_songs} points={upserted_points}")

    print("✅ Done")
    print(f"✅ Songs scanned: {scanned}")
    print(f"✅ Songs ingested/updated: {updated_songs}")
    print(f"✅ Points upserted (chunks): {upserted_points}")


if __name__ == "__main__":
    import os

    # optional: limit crawling for testing
    mp = os.getenv("MAX_PAGES")
    max_pages = int(mp) if mp else None

    main(max_pages=max_pages)
