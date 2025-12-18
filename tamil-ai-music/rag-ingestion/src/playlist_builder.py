from typing import Dict, Any, List, Optional


from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from sentence_transformers import SentenceTransformer

from src.config import QDRANT_URL, COLLECTION, EMBED_MODEL


def collapse_to_unique_songs(hits, k: int):
    best = {}  # song_id -> (score, payload)

    for h in hits:
        payload = h.payload or {}
        sid = payload.get("song_id")
        if not sid:
            continue

        if sid not in best or h.score > best[sid][0]:
            best[sid] = (h.score, payload)

    return sorted(best.items(), key=lambda x: x[1][0], reverse=True)[:k]

def _mood_filter(mood: Optional[str]) -> Optional[Filter]:
    if not mood:
        return None
    return Filter(must=[FieldCondition(key="mood", match=MatchValue(value=mood))])


def get_song_mood(client: QdrantClient, song_id: str) -> Optional[str]:
    # Pull 1 point for this song_id and read mood from payload
    hits = client.scroll(
        collection_name=COLLECTION,
        scroll_filter=Filter(must=[FieldCondition(key="song_id", match=MatchValue(value=song_id))]),
        limit=1,
        with_payload=True,
        with_vectors=False,
    )
    points, _next = hits
    if not points:
        return None
    return (points[0].payload or {}).get("mood")


def get_seed_vector(client: QdrantClient, song_id: str) -> Optional[List[float]]:
    # Grab 1 chunk vector as representative seed (good enough for now)
    hits = client.scroll(
        collection_name=COLLECTION,
        scroll_filter=Filter(must=[FieldCondition(key="song_id", match=MatchValue(value=song_id))]),
        limit=1,
        with_payload=False,
        with_vectors=True,
    )
    points, _next = hits
    if not points:
        return None
    # Qdrant may return vectors as dict or list depending on config
    vec = points[0].vector
    if isinstance(vec, dict):
        # if named vectors used; pick first
        return list(vec.values())[0]
    return vec

def main_query(query: str, k: int = 20, mood: str | None = None):
    """
    Build playlist using a free-text query as the seed.
    1) Embed the query
    2) Search Qdrant
    3) Return k results (optionally filtered by mood)
    """
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBED_MODEL)

    query_vec = model.encode([query], normalize_embeddings=True)[0].tolist()

    q_filter = None
    if mood:
        q_filter = Filter(
            must=[FieldCondition(key="mood", match=MatchValue(value=mood))]
        )

    hits = client.search(
        collection_name=COLLECTION,
        query_vector=query_vec,
        limit=max(k * 8, 50),
        query_filter=q_filter,
        with_payload=True,
    )

    print(f'Query seed: "{query}"')
    if mood:
        print(f"Mood filter: {mood}")
    print(f"Playlist items: {k}")

    items = collapse_to_unique_songs(hits, k)

    print(f"Playlist items: {len(items)}")
    for sid, (score, payload) in items:
        print(
            f"- {score:.4f} "
            f"{payload.get('title')} | "
            f"{payload.get('movie')} | "
            f"mood: {payload.get('mood')} | "
            f"song_id: {sid}"
        )

def collapse_to_unique_songs(hits, k: int):
    best = {}  # song_id -> (score, payload)
    for h in hits:
        payload = h.payload or {}
        sid = payload.get("song_id")
        if not sid:
            continue
        prev = best.get(sid)
        if prev is None or h.score > prev[0]:
            best[sid] = (h.score, payload)

    # sort by score desc and return top-k
    items = sorted(best.items(), key=lambda x: x[1][0], reverse=True)
    return items[:k]


def build_playlist_from_seed(
    seed_song_id: str,
    limit_songs: int = 20,
    oversample_chunks: int = 200,
) -> Dict[str, Any]:
    client = QdrantClient(url=QDRANT_URL)

    mood = get_song_mood(client, seed_song_id)
    if not mood:
        return {"ok": False, "error": "Seed song_id not found in Qdrant", "seed_song_id": seed_song_id}

    seed_vec = get_seed_vector(client, seed_song_id)
    if not seed_vec:
        return {"ok": False, "error": "Could not read seed vector", "seed_song_id": seed_song_id}

    hits = client.search(
        collection_name=COLLECTION,
        query_vector=seed_vec,
        limit=oversample_chunks,
        query_filter=_mood_filter(mood),
        with_payload=True,
    )

    # Dedup by song_id; exclude the seed itself
    best_by_song: Dict[str, Dict[str, Any]] = {}
    """for h in hits:
        p = h.payload or {}
        sid = p.get("song_id")
        if not sid or sid == seed_song_id:
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
            }"""

    playlist = sorted(best_by_song.values(), key=lambda x: x["score"], reverse=True)[:limit_songs]

    return {
        "ok": True,
        "seed_song_id": seed_song_id,
        "mood": mood,
        "count": len(playlist),
        "items": playlist,
    }

def build_playlist_from_query(
    query: str,
    k: int = 20,
    mood: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Build a playlist using a text query instead of a seed song.
    Reuses the same embedding + Qdrant logic as CLI.
    """

    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBED_MODEL)

    query_vector = model.encode(
        query,
        normalize_embeddings=True
    ).tolist()

    search_filter = None
    if mood:
        from qdrant_client.http.models import Filter, FieldCondition, MatchValue
        search_filter = Filter(
            must=[FieldCondition(key="mood", match=MatchValue(value=mood))]
        )

    hits = client.search(
        collection_name=COLLECTION,
        query_vector=query_vector,
        limit=k * 5,  # oversample → dedupe later
        query_filter=search_filter,
    )

    seen_song_ids = set()
    playlist = []

    for h in hits:
        song_id = h.payload.get("song_id")
        if not song_id or song_id in seen_song_ids:
            continue

        seen_song_ids.add(song_id)

        playlist.append({
            "score": round(h.score, 4),
            "song_id": song_id,
            "title": h.payload.get("title"),
            "movie": h.payload.get("movie"),
            "mood": h.payload.get("mood"),
        })

        if len(playlist) >= k:
            break

    return playlist

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m src.playlist_builder",
        description="Build a playlist from a seed song_id OR a free-text query."
    )

    # Backward-compatible positional seed (optional)
    parser.add_argument(
        "seed",
        nargs="?",
        help="Seed song_id (SHA1) (positional). Example: 1b371134... If omitted, use --query."
    )

    # New: query-based seeding
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help='Free-text seed query. Example: "love and longing"'
    )

    # Optional filters / controls
    parser.add_argument(
        "--mood",
        type=str,
        default=None,
        help='Mood filter (e.g., romantic, sad, happy, kuthu, devotional, etc.)'
    )

    parser.add_argument(
        "--k",
        type=int,
        default=None,
        help="Number of playlist items to return."
    )

    # Backward-compatible: allow old 2nd positional arg for k
    # Example: python -m src.playlist_builder <seed_song_id> 15
    parser.add_argument(
        "k_pos",
        nargs="?",
        help="(legacy) Playlist size if using positional args, e.g. 15",
    )

    args = parser.parse_args()

    # Resolve playlist size
    if args.k is not None:
        k = args.k
    elif args.k_pos is not None:
        try:
            k = int(args.k_pos)
        except ValueError:
            raise SystemExit(f"Invalid k value: {args.k_pos!r}. Use an integer like 15.")
    else:
        k = 20

    # Decide mode: seed song_id vs query
    if args.query:
        # You will implement: build seed vector from query embedding
        # Example expected function signature:
        # main_query(query: str, k: int, mood: str | None = None)
        main_query(args.query, k=k, mood=args.mood)
    else:
        if not args.seed:
            raise SystemExit("Provide either a seed song_id (positional) OR --query '...'.")
        main(args.seed, k=k)

