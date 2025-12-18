import os
from dotenv import load_dotenv

load_dotenv()

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "songs_lyrics_v2")


# Embedding model (multilingual works well for Tamil + English)
EMBED_MODEL = os.getenv(
    "EMBED_MODEL",
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)