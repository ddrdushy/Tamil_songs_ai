# tests/test_health.py
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data.get("ok") is True
