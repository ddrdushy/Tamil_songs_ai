from typing import Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from src.config import QDRANT_URL, COLLECTION


def update_song_payload(song_id: str, fields: Dict[str, Any]) -> bool:
    """
    Patch payload fields for ALL chunks of a song_id.
    Used for:
    - youtube_url
    - genre
    - rhythm
    - mood overrides
    """
    if not fields:
        return False

    client = QdrantClient(url=QDRANT_URL)

    client.set_payload(
        collection_name=COLLECTION,
        payload=fields,
        points_selector=Filter(
            must=[
                FieldCondition(
                    key="song_id",
                    match=MatchValue(value=song_id),
                )
            ]
        ),
    )
    return True
