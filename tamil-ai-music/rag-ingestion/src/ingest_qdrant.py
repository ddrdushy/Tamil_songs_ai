from typing import List, Dict, Any
import uuid

from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from sentence_transformers import SentenceTransformer

from src.config import QDRANT_URL, COLLECTION, EMBED_MODEL
from src.preprocess import chunk_text
from src.load_dataset import iter_songs
from src.state_store import StateStore

# Stable namespace UUID for this project (keep constant forever)
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

def make_point_id(song_id: str, chunk_idx: int) -> str:
    # Deterministic UUID based on song_id + chunk index
    return str(uuid.uuid5(NAMESPACE, f"{song_id}:{chunk_idx}"))


def main(dataset_path: str, ingest_limit: int = 50, scan_limit: int = None):
    client = QdrantClient(url=QDRANT_URL)
    model = SentenceTransformer(EMBED_MODEL)
    state = StateStore()
    print("Using state DB:", state.path)
    
    processed = 0
    upserted_points = 0
    scanned = 0


    for song in iter_songs(dataset_path):
        scanned += 1
        
        if scan_limit is not None and scanned >= scan_limit:
            break
        
        prev = state.get(song["song_id"])
        is_new = prev is None
        is_changed = (prev is not None and prev[0] != song["lyrics_hash"])

        if not (is_new or is_changed):
            continue
        
        if is_changed:
            delete_song_chunks(client, song["song_id"])
        # chunk
        chunks = chunk_text(song["lyrics"], chunk_size=1200, overlap=200)
        if not chunks:
            # still update state so we don't keep retrying empty lyrics
            state.upsert(song["song_id"], song["lyrics_hash"], song["meta_hash"])
            continue

        # embed
        vectors = model.encode(chunks, batch_size=32, show_progress_bar=False, normalize_embeddings=True)

        # build points
        points: List[PointStruct] = []
        for i, (chunk, vec) in enumerate(zip(chunks, vectors)):
            payload: Dict[str, Any] = {
                "song_id": song["song_id"],
                "chunk_id": i,
                "mood": song["metadata"].get("mood"),
                "decade": song["metadata"].get("decade"),
                "title": song["metadata"].get("title"),
                "singer": song["metadata"].get("singer"),
                "movie": song["metadata"].get("movie"),
                "year": song["metadata"].get("year"),
                "themes": song["metadata"].get("themes"),
                "chunk_text": chunk,
            }
            points.append(
                PointStruct(
                    id=make_point_id(song["song_id"], i),
                    vector=vec.tolist(),
                    payload=payload,
                )
            )

        # upsert
        client.upsert(collection_name=COLLECTION, points=points)
        upserted_points += len(points)

        # update state AFTER successful upsert
        state.upsert(song["song_id"], song["lyrics_hash"], song["meta_hash"])

        processed += 1
        if processed >= ingest_limit:
            break


    print(f"✅ Rows scanned: {scanned}")
    print(f"✅ Songs ingested: {processed}")
    print(f"✅ Points upserted (chunks): {upserted_points}")

def delete_song_chunks(client: QdrantClient, song_id: str):
    client.delete(
        collection_name=COLLECTION,
        points_selector=Filter(
            must=[FieldCondition(key="song_id", match=MatchValue(value=song_id))]
        ),
    )
    

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.ingest_qdrant <dataset_path> [ingest_limit] [scan_limit]")
        raise SystemExit(1)

    dataset = sys.argv[1]
    ingest_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    scan_limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    main(dataset, ingest_limit=ingest_limit, scan_limit=scan_limit)
