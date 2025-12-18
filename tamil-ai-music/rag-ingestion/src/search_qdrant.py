from typing import Dict, Any, List, Optional
from collections import defaultdict

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from sentence_transformers import SentenceTransformer

from src.config import QDRANT_URL, COLLECTION, EMBED_MODEL


def _mood_filter(mood: Optional[str]) -> Optional[Filter]:
    if not mood:
        return None
    return Filter(
        must=[FieldCondition(key="mood", match=MatchValue(value=mood))]
    )


def search_songs(
    query: str,
    mood: Optional[str] = None,
    top_k_songs: int = 10,
    oversample_chunks: int = 80,
) -> List[Dict[str, Any]]:
    """
    Semantic search in Qdrant by query text.
    Returns UNIQUE songs (dedup by song_id) ranked by best chunk score.
    """
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBED_MODEL)

    qvec = model.encode([query], normalize_embeddings=True)[0].tolist()

    hits = client.search(
        collection_name=COLLECTION,
        query_vector=qvec,
        limit=oversample_chunks,
        query_filter=_mood_filter(mood),
        with_payload=True,
    )

    # Deduplicate by song_id (take best scoring chunk per song)
    best_by_song: Dict[str, Dict[str, Any]] = {}
    for h in hits:
        p = h.payload or {}
        sid = p.get("song_id")
        if not sid:
            continue

        if sid not in best_by_song or h.score > best_by_song[sid]["score"]:
            best_by_song[sid] = {
                "score": float(h.score),
                "song_id": sid,
                "title": p.get("title"),
                "movie": p.get("movie"),
                "year": p.get("year"),
                "mood": p.get("mood"),
                "decade": p.get("decade"),
                "themes": p.get("themes"),
                # Helpful for debugging / UI preview:
                "best_chunk": (p.get("chunk_text") or "")[:240],
            }

    # Sort by score desc, return top K unique songs
    out = sorted(best_by_song.values(), key=lambda x: x["score"], reverse=True)
    return out[:top_k_songs]


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.search_qdrant \"<query>\" [mood]")
        raise SystemExit(1)

    q = sys.argv[1]
    mood = sys.argv[2] if len(sys.argv) > 2 else None

    results = search_songs(q, mood=mood, top_k_songs=10, oversample_chunks=80)
    print(f"Top hits: {len(results)}")
    for r in results:
        print(f"- {r['score']:.4f} {r['title']} | {r['movie']} | mood: {r['mood']}")
