"""
Daily pipeline:
1) Crawl tamil2lyrics -> raw JSONL (append-only)
2) Stream records from raw JSONL, classify (embedding-based), ingest to Qdrant (skip unchanged via state.db)

No YouTube API. No enriched JSONL output needed.
"""

import os
import json
from typing import Dict, Any, Iterator

from scripts import data_scraper
from scripts.json_enhancer import classify_with_embeddings, derive_decade

# 3) reuse your ingestion pieces
from src.ingest_qdrant import main as ingest_main
from pathlib import Path

RAW_JSONL = os.getenv("RAW_JSONL", "data/tamil2lyrics_songs.jsonl")
DATASET_FOR_INGEST = os.getenv("DATASET_FOR_INGEST", RAW_JSONL)
OFFSETS_FILE = Path("data/daily_offsets.json")
TEMP = Path("data/daily_delta_temp.jsonl")


def load_offsets():
    if OFFSETS_FILE.exists():
        return json.loads(OFFSETS_FILE.read_text(encoding="utf-8"))
    return {}


def save_offsets(obj):
    OFFSETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    OFFSETS_FILE.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    
def crawl():
    # Append new songs (resume supported by song_url set) :contentReference[oaicite:4]{index=4}
    data_scraper.scrape_all_json(output_file=RAW_JSONL)


def enrich_in_place_to_temp(raw_path: str, temp_path: str):
    """
    Create a temp JSONL that includes the fields ingestion expects: primary_mood, theme_tags, decade.
    We do NOT call YouTube.
    """
    with open(raw_path, "r", encoding="utf-8") as fin, open(temp_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)

            lyrics_translit = rec.get("english_lyrics") or rec.get("lyrics_translit") or ""
            lyrics_ta = rec.get("tamil_lyrics") or rec.get("lyrics_ta") or ""

            pm, energy, themes, ff = classify_with_embeddings(lyrics_translit, lyrics_ta)  # :contentReference[oaicite:5]{index=5}
            rec["primary_mood"] = pm
            rec["energy_level"] = energy
            rec["theme_tags"] = themes
            rec["is_family_friendly"] = ff
            rec["decade"] = derive_decade(rec.get("movie_year"))

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")


def enrich_delta_to_temp(raw_path: Path, temp_path: Path):
    offsets = load_offsets()
    last_pos = offsets.get(str(raw_path), 0)

    temp_path.parent.mkdir(parents=True, exist_ok=True)

    new_lines = 0
    with open(raw_path, "r", encoding="utf-8") as fin, open(temp_path, "w", encoding="utf-8") as fout:
        fin.seek(last_pos)

        for line in fin:
            line = line.strip()
            if not line:
                continue

            rec = json.loads(line)

            lyrics_translit = rec.get("english_lyrics") or rec.get("lyrics_translit") or ""
            lyrics_ta = rec.get("tamil_lyrics") or rec.get("lyrics_ta") or ""

            pm, energy, themes, ff = classify_with_embeddings(lyrics_translit, lyrics_ta)
            rec["primary_mood"] = pm
            rec["energy_level"] = energy
            rec["theme_tags"] = themes
            rec["is_family_friendly"] = ff
            rec["decade"] = derive_decade(rec.get("movie_year"))

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            new_lines += 1

        offsets[str(raw_path)] = fin.tell()

    save_offsets(offsets)
    print(f"[INFO] Delta classified lines: {new_lines}")
    return new_lines


def run_ingest(temp_jsonl: str):
    # Ingest everything incremental (state.db will skip unchanged)
    ingest_main(temp_jsonl, ingest_limit=999999, scan_limit=None)


if __name__ == "__main__":
    TEMP = "data/daily_enriched_temp.jsonl"
    os.makedirs("data", exist_ok=True)

    print("=== 1) Crawling ===")
    crawl()

    print("=== 2) Classify DELTA (embedding) -> temp JSONL ===")
    new_lines = enrich_delta_to_temp(RAW_JSONL, TEMP)

    if new_lines == 0:
        print("[INFO] No new songs. Skipping ingest.")
    else:
        print("=== 3) Ingest to Qdrant ===")
        run_ingest(str(TEMP))


    print("✅ Daily run complete")
