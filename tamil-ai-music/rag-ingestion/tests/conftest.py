# tests/conftest.py
import os
import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DISABLE_WEB_RESOLVER", "1")

@pytest.fixture(scope="session")
def client():
    os.environ["ENABLE_WEB_RESOLUTION"] = "false"
    os.environ.setdefault("QDRANT_URL", "http://localhost:6333")

    from api.main import app
    from fastapi.testclient import TestClient
    return TestClient(app)