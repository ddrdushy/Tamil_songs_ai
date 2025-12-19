# tests/test_search.py
def test_search_basic(client):
    r = client.get("/search", params={"q": "love and longing", "mood": "romantic", "k": 5})
    assert r.status_code == 200
    data = r.json()

    assert data.get("ok") is True
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) <= 5

    # Optional shape checks (adjust keys to your API response)
    if data["items"]:
        item = data["items"][0]
        assert "score" in item
        assert "song_id" in item
        assert "title" in item


def test_search_missing_query_should_fail(client):
    r = client.get("/search", params={"mood": "romantic", "k": 5})
    # Depending on your FastAPI param validation:
    assert r.status_code in (400, 422)


def test_search_invalid_k_should_fail(client):
    r = client.get("/search", params={"q": "love", "mood": "romantic", "k": 0})
    assert r.status_code in (400, 422)
