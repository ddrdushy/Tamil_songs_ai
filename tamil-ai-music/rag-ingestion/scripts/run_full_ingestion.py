import subprocess
import re
import sys

DATASET = "data/tamil2lyrics_songs_enriched.jsonl"
BATCH_SIZE = 500

def run_once():
    """
    Runs one batch and returns the number of songs ingested.
    """
    cmd = [
        sys.executable,
        "-m",
        "src.ingest_qdrant",
        DATASET,
        str(BATCH_SIZE),
    ]

    print("\n🚀 Running batch ingestion...")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    # Extract "Songs ingested: X"
    match = re.search(r"Songs ingested:\s*(\d+)", result.stdout)
    if not match:
        print("⚠️ Could not detect ingested count. Stopping.")
        return 0

    return int(match.group(1))


def main():
    total = 0
    round_no = 1

    while True:
        print(f"\n===== INGEST ROUND {round_no} =====")
        ingested = run_once()

        # ✅ STOP CONDITION
        if ingested == 0:
            print("\n✅ Ingestion complete. No more songs left.")
            break

        if ingested < BATCH_SIZE:
            print("\n✅ Final partial batch detected. Ingestion complete.")
            total += ingested
            break

        total += ingested
        round_no += 1

    print(f"\n🎉 TOTAL SONGS INGESTED: {total}")

    total = 0
    round_no = 1

    while True:
        print(f"\n===== INGEST ROUND {round_no} =====")
        ingested = run_once()

        if ingested == 0:
            print("\n✅ Ingestion complete. No more songs left.")
            break

        total += ingested
        round_no += 1

    print(f"\n🎉 TOTAL SONGS INGESTED: {total}")


if __name__ == "__main__":
    main()
