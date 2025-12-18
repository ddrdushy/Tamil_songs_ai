from src.load_dataset import iter_songs
from src.state_store import StateStore

DATASET = "../../data/tamil2lyrics_songs_enriched.jsonl"

state = StateStore()

count = 0
new = 0
changed = 0
skipped = 0

for song in iter_songs(DATASET):
    prev = state.get(song["song_id"])

    if prev is None:
        new += 1
        state.upsert(song["song_id"], song["lyrics_hash"], song["meta_hash"])
    else:
        prev_lyrics_hash, prev_meta_hash = prev
        if prev_lyrics_hash != song["lyrics_hash"]:
            changed += 1
            state.upsert(song["song_id"], song["lyrics_hash"], song["meta_hash"])
        else:
            skipped += 1

    count += 1
    if count >= 20:
        break

print("Total checked:", count)
print("New:", new, "Changed:", changed, "Skipped:", skipped)
