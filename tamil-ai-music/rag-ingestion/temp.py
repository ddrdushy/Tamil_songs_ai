import json
from scripts.json_enhancer import classify_with_embeddings, derive_decade

raw = "data/tamil2lyrics_songs.jsonl"
out = "data/tamil2lyrics_songs_enriched.jsonl"

total = 0
classified = 0
skipped_empty = 0

with open(raw, "r", encoding="utf-8") as fin, open(out, "w", encoding="utf-8") as fout:
    for line in fin:
        total += 1
        line = line.strip()
        if not line:
            skipped_empty += 1
            continue

        rec = json.loads(line)

        lyrics_translit = rec.get("english_lyrics") or rec.get("lyrics_translit") or ""
        lyrics_ta = rec.get("tamil_lyrics") or rec.get("lyrics_ta") or ""

        if not lyrics_translit and not lyrics_ta:
            skipped_empty += 1
            continue

        pm, energy, themes, ff = classify_with_embeddings(lyrics_translit, lyrics_ta)
        rec["primary_mood"] = pm
        rec["energy_level"] = energy
        rec["theme_tags"] = themes
        rec["is_family_friendly"] = ff
        rec["decade"] = derive_decade(rec.get("movie_year"))

        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        classified += 1

        if classified % 500 == 0:
            print(f"[ENRICH] processed={classified} / total_read={total}")

print("✅ ENRICH COMPLETE")
print("Total lines read :", total)
print("Classified       :", classified)
print("Skipped empty    :", skipped_empty)
