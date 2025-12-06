def test_root_endpoint_returns_welcome_message(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the ADSP Project API"}


def test_health_check_endpoint_returns_ok_status(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
