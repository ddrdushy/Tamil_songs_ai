def test_player_query_alias(client):
    r = client.get(
        "/player/query",
        params={"q": "love and longing", "mood": "romantic", "k": 5},
    )
    assert r.status_code == 200
    data = r.json()
    assert "items" in data
    assert isinstance(data["items"], list)


def test_player_seed_alias(client):
    # get a seed song via search
    r1 = client.get("/search", params={"q": "love", "mood": "romantic", "k": 1})
    assert r1.status_code == 200
    items = r1.json()
    assert len(items["items"]) > 0

    seed_song_id = items["items"][0]["song_id"]

    r2 = client.get(f"/player/seed/{seed_song_id}", params={"k": 5})
    assert r2.status_code == 200
    data = r2.json()
    assert "items" in data
    assert isinstance(data["items"], list)
