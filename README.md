# 🎵 Tamil Lyrics RAG & Playlist Engine

A production-ready **Tamil song lyrics intelligence system** built using **RAG (Retrieval-Augmented Generation)** concepts.

This project:
- Crawls Tamil song lyrics
- Enriches them using **embedding-based classification**
- Stores semantic chunks in **Qdrant**
- Enables **semantic search** and **playlist generation**
- Exposes everything via a **simple FastAPI layer**
- Includes **unit tests** for core logic

---

## 🚀 What This Project Does

### Core Capabilities
- 🔍 Semantic search over Tamil lyrics
- ❤️ Mood-aware discovery (romantic, sad, happy, etc.)
- 🎧 Playlist generation from:
  - A **seed song**
  - A **natural language query**
- ⚡ Incremental ingestion (state-tracked)
- 🧪 Unit-tested core logic
- 🌐 REST API for frontend / integration

---

## 🧠 Architecture Overview

```text
Tamil2Lyrics (crawl)
        ↓
Lyrics text
        ↓
Chunking + Embeddings
        ↓
Qdrant Vector DB
        ↓
Search / Playlist Logic
        ↓
FastAPI
````

---

## 📁 Project Structure

```text
rag-ingestion/
├── api/                     # FastAPI layer
│   ├── main.py
│   └── __init__.py
│
├── src/                     # Core domain logic
│   ├── ingest_qdrant.py
│   ├── search_qdrant.py
│   ├── playlist_builder.py
│   ├── preprocess.py
│   ├── load_dataset.py
│   ├── state_store.py
│   ├── create_collection.py
│   └── config.py
│
├── scripts/                 # CLI / pipelines
│   ├── crawl_ingest_direct.py
│   ├── crawl.py
│   ├── enrich.py
│   ├── run_full_ingestion.py
│   └── debug_reingest_loop.py
│
├── tests/                   # Unit tests
│   ├── test_search.py
│   ├── test_playlist.py
│   ├── test_health.py
│   └── test_core_point_id.py
│
├── data/                    # Local data & state
│   ├── raw/
│   ├── enriched/
│   ├── qdrant/
│   └── state_v2.db
│
├── archive/                 # Old scripts & backups
├── docs/                    # Screenshots / docs
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🔁 Data Flow (Current Scope)

### 1️⃣ Crawl

* Scrapes lyrics from **tamil2lyrics.com**
* No YouTube, no external APIs
* Resume-safe via URL tracking

```bash
python -m scripts.crawl_ingest_direct
```

---

### 2️⃣ Enrich (Embedding-Based)

* Uses **Sentence Transformers**
* Derives:

  * mood
  * themes
  * decade
  * energy
* No LLM calls required

---

### 3️⃣ Ingest into Qdrant

* Lyrics are chunked
* Each chunk embedded
* Stored with metadata
* State tracked via SQLite (`state_v2.db`)

---

## 🔍 Semantic Search

### CLI

```bash
python -m src.search_qdrant "love and longing" romantic
```

### API

```http
GET /search?q=love and longing&mood=romantic&k=10
```

---

## 🎧 Playlist Builder

### From Seed Song

```bash
python -m src.playlist_builder <song_id> 15
```

### From Query

```bash
python -m src.playlist_builder --query "love and longing" --mood romantic --k 15
```

### API

```http
GET /playlist/seed/{song_id}?k=15
GET /playlist/query?q=love+and+longing&mood=romantic&k=15
```

Playlist logic:

* Vector similarity
* Mood-filtered
* Deduplicated by song
* Ranked by best chunk score

---

## 🌐 API Layer

### Run API

```bash
python -m uvicorn api.main:app --reload
```

### Available Endpoints

* `/health`
* `/search`
* `/playlist/seed/{song_id}`
* `/playlist/query`

---

## 🧪 Testing

Run all tests:

```bash
pytest
```

Covers:

* Search results
* Playlist ranking
* Deterministic point IDs
* API health

---

## 🧩 Tech Stack

* **Python 3.10**
* **SentenceTransformers**
* **Qdrant**
* **FastAPI**
* **SQLite**
* **Pytest**

---

## ✅ Current Status

* ✔ Architecture stable
* ✔ API functional
* ✔ Playlist logic validated
* ✔ Tested with partial crawl (1 page)
* ⏳ Full crawl ingestion can be run later

---

## 🧭 Next Logical Enhancements (Optional)

* Pagination in search
* Diversity boosting in playlists
* API response schemas (Pydantic)
* Frontend / UI
* CI pipeline

---

## ⚠️ Scope Note

This repository **only covers**:

* Lyrics intelligence
* Search & playlist generation

**Radio mode, streaming, IVR, or audio playback are explicitly out of scope** and belong to a separate product.

---

## 🤝 License

Internal / Experimental
Use responsibly.
