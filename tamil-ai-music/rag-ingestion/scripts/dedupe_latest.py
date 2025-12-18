import json
from pathlib import Path

SRC = Path("data/tamil2lyrics_songs_enriched.jsonl")
OUT = Path("data/tamil2lyrics_songs_enriched_latest.jsonl")
import hashlib

def stable_song_id(song_url: str) -> str:
    return hashlib.sha1(song_url.encode("utf-8")).hexdigest()

def main():
    # Keep only the LAST occurrence per song_id
    latest = {}
    total = 0
    bad = 0

    with SRC.open("r", encoding="utf-8") as f:
        for line in f:
            total += 1
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue

            # IMPORTANT: must match what src/load_dataset.py uses for song_id fields
            # If your loader uses song_url hashing, use that same id field here.
            song_id = rec.get("song_id")
            if not song_id:
                song_url = rec.get("song_url", "")
                if not song_url:
                    continue
                song_id = stable_song_id(song_url)
                rec["song_id"] = song_id

            latest[song_id] = rec

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as w:
        for rec in latest.values():
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("✅ DEDUPE DONE")
    print("Total lines read :", total)
    print("Bad lines        :", bad)
    print("Unique song_ids  :", len(latest))
    print("Wrote            :", str(OUT))

if __name__ == "__main__":
    main()
