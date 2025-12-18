
# 🎧 Tamil AI Music – RAG Ingestion & Search Backend

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Qdrant](https://img.shields.io/badge/VectorDB-Qdrant-brightgreen)
![RAG](https://img.shields.io/badge/AI-RAG-orange)
![Embeddings](https://img.shields.io/badge/Embeddings-SentenceTransformers-purple)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)
![License](https://img.shields.io/badge/License-MIT-lightgrey)


This project builds an **AI-powered music intelligence backend** for Tamil songs using **lyrics-based RAG (Retrieval Augmented Generation)**.

It crawls Tamil song lyrics, enriches them with AI-derived metadata (mood, themes, decade), embeds them into a **vector database (Qdrant)**, and keeps the system **incrementally updatable** for daily runs.

This backend powers a **web-based music player** that can:
- Search songs by lyrics or vibe
- Build strict mood-based queues
- Recommend the next songs intelligently
- Play songs via YouTube embeds (lazy lookup)

---

## 🧠 High-Level Architecture

```

Crawler → Enrichment (AI) → Canonical Dataset → Vector DB (Qdrant)
↑
State DB

```

---

## 📁 Project Structure

```

rag-ingestion/
├── data/
│   ├── tamil2lyrics_songs.jsonl                # Raw crawl output
│   ├── tamil2lyrics_songs_enriched.jsonl       # Enriched (may contain history)
│   ├── tamil2lyrics_songs_enriched_latest.jsonl# Canonical latest-only dataset
│   └── state.db                                # Ingestion state (SQLite)
│
├── src/
│   ├── ingest_qdrant.py        # Main ingestion logic
│   ├── load_dataset.py        # Dataset loader & hashing
│   ├── create_collection.py   # Qdrant collection setup
│   └── state_store.py         # SQLite state tracking
│
├── scripts/
│   ├── data_scraper.py            # Lyrics crawler
│   ├── json_enhancer.py           # AI enrichment (embedding-based classifier)
│   ├── dedupe_enriched_latest.py  # Deduplicate to canonical dataset
│   ├── run_full_ingestion.py      # Batch runner until ingestion completes
│   └── daily_run.py               # Crawl → enrich → ingest (delta)
│
├── docker-compose.yml             # Qdrant container
├── requirements.txt
└── README.md

````

---

## 🧾 Dataset Format (Canonical)

Each song record contains:

```json
{
  "song_id": "sha1(song_url)",
  "song_title": "...",
  "song_url": "...",
  "movie_title": "...",
  "movie_year": "2015",
  "singer": "...",
  "music_by": "...",
  "english_lyrics": "...",
  "tamil_lyrics": "...",
  "primary_mood": "romantic",
  "energy_level": "medium",
  "theme_tags": "longing,yearning",
  "is_family_friendly": true,
  "decade": "2010s"
}
````

> **song_id is deterministic**: `sha1(song_url)`

---

## 🚀 Setup Instructions

### 1️⃣ Start Qdrant

```bash
docker compose up -d
```

Qdrant UI: [http://localhost:6333](http://localhost:6333)

---

### 2️⃣ Crawl Lyrics

```bash
python -m scripts.data_scraper
```

Output:

```
data/tamil2lyrics_songs.jsonl
```

---

### 3️⃣ Enrich with AI Metadata

```bash
python -m scripts.json_enhancer
```

Output:

```
data/tamil2lyrics_songs_enriched.jsonl
```

---

### 4️⃣ Create Canonical Latest-Only Dataset

```bash
python -m scripts.dedupe_enriched_latest
```

Output:

```
data/tamil2lyrics_songs_enriched_latest.jsonl
```

---

### 5️⃣ Reset Ingestion State (first run only)

```bash
rm data/state.db
```

---

### 6️⃣ Ingest into Qdrant (Full Run)

```bash
python -m scripts.run_full_ingestion
```

This:

* Embeds lyrics in chunks
* Upserts into Qdrant
* Tracks progress via `state.db`
* Stops automatically when complete

---

### 7️⃣ Verify

```bash
sqlite3 data/state.db "select count(*) from song_state;"
```

Expected:

```
19932
```

---

## 🔁 Daily Update Flow (Production)

Run once per day:

```bash
python -m scripts.daily_run
```

This will:

1. Crawl new songs (append-only)
2. Enrich only new/changed lyrics
3. Deduplicate to latest-only
4. Ingest only new/changed songs into Qdrant

---

## 🎧 How This Powers the Music Player

The music player will:

1. Embed user search query or selected song lyrics
2. Query Qdrant with:

   * Vector similarity
   * Strict mood filter
3. Build a smart queue
4. Play via YouTube embed (URL resolved lazily)

---

## 🔐 Design Principles

* Deterministic IDs (no Python `hash()`)
* Append-only raw data
* Canonical latest-only ingestion
* Resumable & crash-safe
* Scales beyond 100K songs

---

## 🧭 Next Steps

* Search API (lyrics / vibe)
* Strict mood playlist builder
* Web music player UI
* YouTube lazy lookup & caching


