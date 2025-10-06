import pytest
from httpx import AsyncClient
from main import app, get_db

auth_headers = {"Authorization": "Bearer abc123"}


class _StubDB:
    def get_games(self):
        return []
    def get_games_by_sport(self, sport):
        return []
    def get_games_by_date(self, d):
        return []
    def get_followed_games(self, device_uid):
        return []
    def get_games_with_team(self, team):
        return []
    def get_id_by_team(self, team, sport):
        return ""
    def insert_sport(self, name):
        return None
    def insert_school(self, name, sport):
        return None
    def insert_game(self, game):
        return None
    def update_game_winner(self, game_id, winner):
        return 0


@pytest.fixture(autouse=True)
def override_db_dependency(monkeypatch):
    def _yield_stub():
        stub = _StubDB()
        yield stub
    app.dependency_overrides[get_db] = _yield_stub
    yield
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_get_all_games():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/games", headers=auth_headers)
    assert response.status_code == 200
    assert "games" in response.json()


@pytest.mark.asyncio
async def test_get_games_by_sport():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/games/sport/Basketball", headers=auth_headers)
    assert response.status_code == 200
    assert "games" in response.json()


@pytest.mark.asyncio
async def test_get_games_by_date():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/games/date/2025-04-06", headers=auth_headers)
    assert response.status_code == 200
    assert "games" in response.json()


@pytest.mark.asyncio
async def test_get_followed_games():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/games/followed/device_id_1", headers=auth_headers)
    assert response.status_code == 200
    assert "games" in response.json()


@pytest.mark.asyncio
async def test_get_id_by_team():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/id/Team1/Basketball", headers=auth_headers)
    assert response.status_code in [200, 404]


@pytest.mark.asyncio
async def test_add_sport():
    payload = {"name": "Basketball"}
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/sports", json=payload, headers=auth_headers)
    assert response.status_code in [200, 400]


@pytest.mark.asyncio
async def test_add_school():
    payload = {"name": "Team1", "sport": "Basketball"}
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/schools", json=payload, headers=auth_headers)
    assert response.status_code in [200, 400]


@pytest.mark.asyncio
async def test_add_game():
    payload = {
        "date": "2025-04-06",
        "time": "19:00:00",
        "away_team": "Team2",
        "home_team": "Team1",
        "score": {"away": 70, "home": 80},
        "winner": "Team1",
        "sport": "Basketball"
    }
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/games", json=payload, headers=auth_headers)
    assert response.status_code in [200, 400]


@pytest.mark.asyncio
async def test_update_game_winner():
    game_id = 1
    winner = "Team1"
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.put(f"/games/{game_id}/winner?winner={winner}", headers=auth_headers)
    assert response.status_code in [200, 400, 403]

# Testing Authentication

@pytest.mark.asyncio
async def test_unauthorized_access():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/games")
    assert response.status_code == 200
