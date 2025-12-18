from src.load_dataset import iter_songs
from src.state_store import StateStore

DATASET = "data/tamil2lyrics_songs_enriched_latest.jsonl"  # adjust if needed

def main():
    state = StateStore()
    changed = []

    for song in iter_songs(DATASET):
        prev = state.get(song["song_id"])
        is_new = prev is None
        is_changed = (prev is not None and prev[0] != song["lyrics_hash"])

        if is_new or is_changed:
            changed.append((song["song_id"], song["lyrics_hash"][:12], None if prev is None else prev[0][:12]))
            if len(changed) >= 40:  # show more than 20
                break

    print("First candidates (song_id, current_lyrics_hash, prev_lyrics_hash):")
    for row in changed:
        print(row)

if __name__ == "__main__":
    main()
