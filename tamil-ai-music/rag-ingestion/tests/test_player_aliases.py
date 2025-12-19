def test_player_query_alias(client):
    r = client.get("/player/query", params={"q": "love", "mood": "romantic", "k": 3})
    assert r.status_code == 200
    data = r.json()
    assert "items" in datadef test_player_query_alias(client):
    r = client.get("/player/query", params={"q": "love", "mood": "romantic", "k": 3})
    assert r.status_code == 200
    data = r.json()
    assert "items" in data