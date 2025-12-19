# tests/conftest.py
import os
import pytest
from fastapi.testclient import TestClient

@pytest.fixture(scope="session")
def client():
    # Ensure we use local qdrant
    os.environ.setdefault("QDRANT_URL", "http://localhost:6333")

    # Import AFTER env set
    from api.main import app  # noqa

    return TestClient(app)
