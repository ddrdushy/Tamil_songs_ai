# tests/test_core_point_id.py
from src.ingest_qdrant import make_point_id

def test_make_point_id_is_deterministic():
    a = make_point_id("song123", 0)
    b = make_point_id("song123", 0)
    assert a == b

def test_make_point_id_differs_by_chunk():
    a = make_point_id("song123", 0)
    b = make_point_id("song123", 1)
    assert a != b
