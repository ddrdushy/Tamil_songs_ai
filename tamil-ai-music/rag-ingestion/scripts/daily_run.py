"""
Daily pipeline (clean structure):
1) Crawl tamil2lyrics -> data/raw/songs_raw.jsonl (append-only)
2) Classify (embedding-based) -> temp enriched JSONL (delta or full)
3) Ingest to Qdrant using src.ingest_qdrant (state.db prevents duplicates)

Modes:
- MODE=delta (default): only classify newly appended lines using file byte offset
- MODE=full: re-classify entire raw file and ingest everything (optionally reset state.db)

Usage:
  python -m scripts.daily_run
  MODE=full RESET_STATE=1 python -m scripts.daily_run
"""

import os
import json
from pathlib import Path

from scripts import crawl as crawler
from scripts import enrich as enricher

from src.ingest_qdrant import main as ingest_main


# Canonical paths
RAW_JSONL = Path(os.getenv("RAW_JSONL", "data/raw/songs_raw.jsonl"))
TEMP_ENRICHED = Path(os.getenv("TEMP_ENRICHED", "data/enriched/daily_enriched_temp.jsonl"))
OFFSETS_FILE = Path(os.getenv("OFFSETS_FILE", "data/state/daily_offsets.json"))
MODE = os.getenv("MODE", "delta").lower()          # delta | full
RESET_STATE = os.getenv("RESET_STATE", "0") == "1" # when MODE=full, optionally reset state db


def load_offsets() -> dict:
    if OFFSETS_FILE.exists():
        return json.loads(OFFSETS_FILE.read_text(encoding="utf-8"))
    return {}


def save_offsets(obj: dict):
    OFFSETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    OFFSETS_FILE.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def classify_record(rec: dict) -> dict:
    # Use enrich module to classify and also ensure stable song_id + hashes if your loader expects them
    return enricher.enrich_one_record(rec)


def enrich_full_to_temp(raw_path: Path, temp_path: Path) -> int:
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with raw_path.open("r", encoding="utf-8") as fin, temp_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rec = classify_record(rec)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            total += 1
    print(f"[INFO] Full classified lines: {total}")
    return total


def enrich_delta_to_temp(raw_path: Path, temp_path: Path) -> int:
    offsets = load_offsets()
    last_pos = offsets.get(str(raw_path), 0)

    temp_path.parent.mkdir(parents=True, exist_ok=True)

    new_lines = 0
    with raw_path.open("r", encoding="utf-8") as fin, temp_path.open("w", encoding="utf-8") as fout:
        fin.seek(last_pos)

        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rec = classify_record(rec)
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            new_lines += 1

        offsets[str(raw_path)] = fin.tell()

    save_offsets(offsets)
    print(f"[INFO] Delta classified lines: {new_lines}")
    return new_lines


def maybe_reset_state_db():
    # If you want a clean ingest in full mode, delete state db explicitly.
    # Update this path if your StateStore uses a different filename.
    state_db = Path("data/state/state.db")
    if state_db.exists():
        print(f"[WARN] RESET_STATE=1 => deleting {state_db}")
        state_db.unlink()


def run_ingest(temp_jsonl: Path):
    # NOTE: ingest_main signature = main(dataset_path, ingest_limit=..., scan_limit=...)
    ingest_main(str(temp_jsonl), ingest_limit=999999999, scan_limit=None)


if __name__ == "__main__":
    # Ensure folders exist
    RAW_JSONL.parent.mkdir(parents=True, exist_ok=True)
    TEMP_ENRICHED.parent.mkdir(parents=True, exist_ok=True)
    Path("data/state").mkdir(parents=True, exist_ok=True)

    print("=== 1) Crawling ===")
    crawler.scrape_all_json(output_file=str(RAW_JSONL))

    if MODE == "full":
        print("=== 2) Classify FULL (embedding) -> temp enriched JSONL ===")
        if RESET_STATE:
            maybe_reset_state_db()
        count = enrich_full_to_temp(RAW_JSONL, TEMP_ENRICHED)
    else:
        print("=== 2) Classify DELTA (embedding) -> temp enriched JSONL ===")
        count = enrich_delta_to_temp(RAW_JSONL, TEMP_ENRICHED)

    if count == 0:
        print("[INFO] No new songs. Skipping ingest.")
    else:
        print("=== 3) Ingest to Qdrant ===")
        run_ingest(TEMP_ENRICHED)

    print("✅ Daily run complete")
