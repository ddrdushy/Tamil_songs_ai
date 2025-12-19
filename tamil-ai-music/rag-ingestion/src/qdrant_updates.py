from __future__ import annotations
from typing import Dict, Any
from datetime import datetime, timezone

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest

from src.config import QDRANT_URL, COLLECTION


def patch_song_payload(song_id: str, patch: Dict[str, Any]) -> None:
    client = QdrantClient(url=QDRANT_URL)

    patch = dict(patch)
    patch["youtube_updated_at"] = datetime.now(timezone.utc).isoformat()

    client.set_payload(
        collection_name=COLLECTION,
        payload=patch,
        points_selector=rest.FilterSelector(
            filter=rest.Filter(
                must=[
                    rest.FieldCondition(
                        key="song_id",
                        match=rest.MatchValue(value=song_id),
                    )
                ]
            )
        ),
    )
