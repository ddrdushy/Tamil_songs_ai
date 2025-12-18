from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PayloadSchemaType
from sentence_transformers import SentenceTransformer


from src.config import QDRANT_URL, COLLECTION, EMBED_MODEL



def main():
    # Load model just to get embedding dimension safely
    model = SentenceTransformer(EMBED_MODEL)
    dim = model.get_sentence_embedding_dimension()

    client = QdrantClient(url=QDRANT_URL)

    # Recreate for now (dev). Later we’ll make this safer.
    if client.collection_exists(COLLECTION):
        client.delete_collection(collection_name=COLLECTION)

    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
    )

    # Payload indexes for filtering
    client.create_payload_index(
        collection_name=COLLECTION,
        field_name="song_id",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    client.create_payload_index(
        collection_name=COLLECTION,
        field_name="mood",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    client.create_payload_index(
        collection_name=COLLECTION,
        field_name="decade",
        field_schema=PayloadSchemaType.KEYWORD,
    )

    print(f"✅ Created Qdrant collection '{COLLECTION}' with dim={dim}")


if __name__ == "__main__":
    main()
