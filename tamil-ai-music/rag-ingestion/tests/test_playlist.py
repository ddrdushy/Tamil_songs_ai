# tests/test_playlist.py

def _get_any_seed_song_id(client) -> str:
    # Use search to pick a song_id from your DB (works even with small dataset)
    r = client.get("/search", params={"q": "love", "mood": "romantic", "k": 1})
    assert r.status_code == 200
    data = r.json()
    assert data.get("ok") is True
    assert data["items"], "No songs found to use as seed. Ingest at least 1 page."
    return data["items"][0]["song_id"]


def test_playlist_from_seed(client):
    seed = _get_any_seed_song_id(client)

    r = client.get(f"/playlist/seed/{seed}", params={"k": 5})
    assert r.status_code == 200
    data = r.json()

    assert data.get("ok") is True
    assert data.get("seed_song_id") == seed
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) <= 5

    # playlist should not include the seed itself (depending on your logic)
    for it in data["items"]:
        assert it["song_id"] != seed


def test_playlist_from_seed_invalid_song(client):
    r = client.get("/playlist/seed/does-not-exist", params={"k": 5})
    # should be 404 ideally, not 500
    assert r.status_code in (400, 404, 422)

# tests/test_playlist.py (add this only if endpoint exists)
def test_playlist_from_query(client):
    r = client.get("/playlist/query", params={"q": "love and longing", "mood": "romantic", "k": 5})
    assert r.status_code == 200
    data = r.json()
    assert data.get("ok") is True
    assert "items" in data
    assert len(data["items"]) <= 5