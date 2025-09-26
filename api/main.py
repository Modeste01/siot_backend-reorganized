from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Header
from db import Database
from typing import Dict, List, Any, Union, Generator
import os
import uvicorn
import datetime
import asyncio
import asyncpg
from fastapi.responses import HTMLResponse
import httpx
import json
import os
# from dotenv import load_dotenv 

# load_dotenv()

DATABASE_NAME = os.getenv('DB_NAME', 'sportsiot')
DATABASE_HOST = os.getenv('DB_HOST', 'db')
DATABASE_PORT = os.getenv('DB_PORT', '5432')

AUTH_TOKEN = 'abc123'
from datetime import date

class WebsocketConnections:

    def __init__(self):
        self.activeConnections = []  # legacy list (kept for compatibility)
        # Map: user_id (str) -> { 'ws': WebSocket, 'school': str | None, 'sports': list[str] }
        self.connectionsPreferences = {}

    async def connect(self, websocket: WebSocket, client_id):
        try:
            await websocket.accept()
            self.activeConnections.append(websocket)
            self.connectionsPreferences[str(client_id)] = {
                'ws': websocket,
                'school': None,
                'sports': []
            }
        except Exception as e:
            print(e)

    def disconnect(self, websocket: WebSocket):
        try:
            self.activeConnections.remove(websocket)
        except ValueError:
            pass
        to_delete = None
        for uid, info in self.connectionsPreferences.items():
            if info.get('ws') is websocket:
                to_delete = uid
                break
        if to_delete:
            self.connectionsPreferences.pop(to_delete, None)

    async def broadcastAll(self, payload):
        for connections in self.activeConnections:
            await connections.send_text(payload)

    # def sendReply(self, clientID, payload):
    #     """Disabled pending redesign; legacy code contained errors and undefined vars."""
    #     pass

    async def broadcastWin(self, winningTeam):
        """Broadcast a simple winner message to all active connections.
        Expects winningTeam as a JSON string with keys 'winner' and 'sport'."""
        try:
            winner = json.loads(winningTeam)
        except Exception:
            winner = {"winner": str(winningTeam), "sport": ""}
        message = f"{winner.get('winner','')}, {winner.get('sport','')}"
        for ws in self.activeConnections:
            await ws.send_text(message)

    async def broadcast_to_users(self, payload):
        # Forward only final updates (winner present) to interested users registered via WS
        try:
            payload_obj = json.loads(payload) if isinstance(payload, str) else payload
        except Exception:
            return

        winner = payload_obj.get('winner')
        if winner in (None, ""):
            return  # Only push when a game goes final

        # Normalize time fields to Z when possible
        if 'time' in payload_obj and isinstance(payload_obj['time'], str):
            payload_obj['time'] = _normalize_time_to_z(payload_obj['time'])

        home_team = payload_obj.get('home_team')
        away_team = payload_obj.get('away_team')
        sport = payload_obj.get('sport')

        for uid, info in list(self.connectionsPreferences.items()):
            ws = info.get('ws')
            school = info.get('school')
            sports = info.get('sports') or []
            if ws is None:
                continue
            if sport in sports and (school == home_team or school == away_team):
                try:
                    await ws.send_text(json.dumps(payload_obj))
                except Exception as e:
                    print(f"WS send error for user {uid}: {e}")
                    self.disconnect(ws)

    def register_preferences(self, user_id: str, school: str, sports: list[str]):
        entry = self.connectionsPreferences.get(str(user_id))
        if entry is not None:
            entry['school'] = school
            entry['sports'] = sports or []
            self.connectionsPreferences[str(user_id)] = entry

manager = WebsocketConnections()

# Hardcoded device authentication
DEVICE_AUTH = {
    "device_id_1": {"key": "devicekey", "role": "device"},
    "admin_id_1": {"key": "adminkey", "role": "admin"}
}

app = FastAPI()

def get_db():
    db = Database(
        DATABASE_NAME,
        "root",
        "root",
        DATABASE_HOST,
        DATABASE_PORT,
    )
    try:
        yield db
    finally:
        # db.close()
        pass

def _normalize_time_to_z(val: str) -> str:
    if not isinstance(val, str) or len(val) == 0:
        return val
    s = val.replace(' ', 'T')
    if s.endswith('Z'):
        return s
    if s.endswith('+00:00'):
        return s[:-6] + 'Z'
    # includes other offsets like +HH:MM or -HH:MM -> leave as-is for clients that handle offsets
    # if no timezone info, append Z assuming UTC
    if '+' not in s and '-' not in s[10:]:  # avoid leading date '-'
        return s + 'Z'
    return s

# Removed unused /user_id/ routes that referenced undefined variables.

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(websocket, user_id)
    try:
        headers = websocket.headers
        auth_header = headers.get('authorization')
        if auth_header:
            verify_device_auth(auth_header)
        else:
            token = websocket.query_params.get('authorization')
            if token != AUTH_TOKEN:
                raise Exception()
        # Expect the first message to be JSON: { "uid": "...", "school": "TeamX", "sports": ["Basketball", ...] }
        first = await websocket.receive_text()
        try:
            reg = json.loads(first)
            uid = str(reg.get('uid', user_id))
            school = reg.get('school')
            sports = reg.get('sports', [])
            manager.register_preferences(uid, school, sports)
        except Exception as e:
            await websocket.send_text(json.dumps({"error": "invalid registration"}))
        # Send initial state: all games in the last 24 hours for the school across requested sports
        db = next(get_db())
        try:
            init_games = db.get_recent_games_for_team_by_sports(school, sports, hours=24)
        except Exception as e:
            init_games = []
        # Normalize times to Z for clients
        for g in init_games:
            if isinstance(g, dict) and 'time' in g and isinstance(g['time'], str):
                g['time'] = _normalize_time_to_z(g['time'])
        await websocket.send_text(json.dumps({"init": True, "games": init_games}))
        # Now keep the socket alive, responding to pings
        while True:
            message = await websocket.receive()
            if "bytes" in message and message["bytes"] is not None:
                await websocket.send({"type": "websocket.pong"})  # Send Pong
    
    except Exception as e:
            print(f"WebSocket error: {e}")
    finally:
        await websocket.close()


# Dependency for device-based authentication
def verify_device_auth(authorization: str = Header(None)):
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail='Unauthorized')
    token = authorization[7:]
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail='Forbidden')
    return "admin"
    # return device["role"]

# --- GET ENDPOINTS --- #

# Retrieve list of all games
@app.get("/games")
def get_games(db=Depends(get_db)):
    data = db.get_games()
    return {"games": data}

# Removed broken /games/{team}/{sport}/{date} route (no matching function and DB call). Add later if needed.

# Retrieve list of games by team
@app.get("/games/{team}")
def get_games_with_team(team, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_with_team(team)
    return {"games": data}

# Retrieve list of games by sport
@app.get("/games/sport/{sport}")
def get_games_by_sport(sport: str, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_by_sport(sport)
    return {"games": data}

# Retrieve list of games by date
@app.get("/games/date/{date}")
def get_games_by_date(date: str, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_by_date(date)
    return {"games": data}

# Retrieve list of games by time
@app.get("/games/time/{time}")
def get_games_by_time(time: str, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_by_time(time)
    return {"games": data}

# Retrieve list of games by BOTH date and time
@app.get("/games/date/{date}/time/{time}")
def get_games_by_date_and_time(date: str, time: str, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_by_date_and_time(date, time)
    return {"games": data}

# Retrieve list of games with min_score by a team
@app.get("/games/score/{min_score}")
def get_games_by_score(min_score: int, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_games_by_score(min_score)
    return {"games": data}

# Get a single game by id
@app.get("/games/id/{game_id}")
def get_game_by_id(game_id: int, db=Depends(get_db), role=Depends(verify_device_auth)):
    data = db.get_game_by_id(game_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return data

# Retrieve list of games on current day
@app.get("/games/today")
def get_games_today(db=Depends(get_db), role=Depends(verify_device_auth)):
    today = date.today().isoformat()
    data = db.get_games_by_date(today)
    return {"games":data}

# Retrieve list of teams playing on current day
@app.get("/teams/today")
def get_teams_playing_today(db=Depends(get_db), role=Depends(verify_device_auth)):
    today = date.today().isoformat()
    teams = db.get_teams_playing_on_date(today)
    return {"teams": teams}

# Retrieve list of sports being played on current day
@app.get("/sports/today")
def get_sports_playing_today(db=Depends(get_db), role=Depends(verify_device_auth)):
    today = date.today().isoformat()
    sports = db.get_sports_playing_on_date(today)
    return {"sports": sports}

# -- FOR DeviceUser TABLE -- #

# Retrieve list of games for specific device - by followed school and sport
@app.get("/games/followed/{device_uid}")
def get_followed_games(device_uid: str, db=Depends(get_db)):
    data = db.get_followed_games(device_uid)
    return {"games": data}

@app.get("/id/{team}/{sport}")
def get_id_by_team(team, sport, db=Depends(get_db)):
    data = db.get_id_by_team(team, sport)
    if not data:
        raise HTTPException(status_code=404, detail=data)
    return str(data)

# --- POST ENDPOINTS --- #

# Add a game to the database (admin-only)
@app.post("/games")
def add_game(game: dict, role=Depends(verify_device_auth), db=Depends(get_db)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    try:
        db.insert_game(game)
        return {"message": "Game added successfully"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/schools")
def add_school(data: dict, role=Depends(verify_device_auth), db=Depends(get_db)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    try:
        db.insert_school(data['name'], data['sport'])
        return {"message": "School added successfully"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/sports")
def add_sport(data: dict, role=Depends(verify_device_auth), db=Depends(get_db)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    try:
        print(data)
        db.insert_sport(data['name'])
        return {"message": "School added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# --- PUT ENDPOINTS --- #

    
# --- DELETE ENDPOINTS --- #

# Delete game by game ID
@app.delete("/games/id/{game_id}")
def delete_game_by_id(game_id: int, db=Depends(get_db)):
    try:
        deleted = db.delete_game_by_id(game_id)
        if deleted:
            return {"message": "Game deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Game not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- DEVICE FOLLOW PREFERENCES --- #

@app.post("/deviceuser/follow")
def follow_school(payload: dict, db=Depends(get_db), role=Depends(verify_device_auth)):
    try:
        uid = int(payload['uid'])
        school = payload['followed_school']
        sport = payload['followed_sport']
        db.set_follow(uid, school, sport)
        return {"message": "Follow set"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/deviceuser/follow")
def unfollow_school(uid: int, followed_school: str, followed_sport: str, db=Depends(get_db), role=Depends(verify_device_auth)):
    try:
        db.delete_follow(uid, followed_school, followed_sport)
        return {"message": "Follow deleted"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# listen for any changes to the tables
@app.on_event("startup")
async def startup():
    # Skip DB listener in NO_DB mode
    if os.getenv('NO_DB', '0') in ('1', 'true', 'True'):
        print('NO_DB set; skipping Postgres LISTEN task')
        return
    try:
        asyncio.create_task(listen_to_postgres())
    except Exception as e:
        print(f"Failed to start Postgres listener: {e}")

async def listen_to_postgres():
    conn = None
    # Retry until DB is available
    while conn is None:
        try:
            conn = await asyncpg.connect(
                user="root",
                password="root",
                database=DATABASE_NAME,
                host=DATABASE_HOST,
                port=int(DATABASE_PORT)
            )
        except Exception as e:
            print(f"Postgres not ready ({e}); retrying in 3s...")
            await asyncio.sleep(3)
    try:
        await conn.add_listener("notify_channel", notify_handler)
        print('listening on notify_channel')
        while True:
            await asyncio.sleep(10)  # Keep alive
    except Exception as e:
        print(f"Error in Postgres listener: {e}")

def notify_handler(conn, pid, channel, payload):
    asyncio.create_task(manager.broadcast_to_users(payload))
    # asyncio.create_task(manager.broadcastAll(payload))
    # asyncio.create_task(manager.broadcastWin(payload))
    # jsonObjs = json.load(payload)
    # manager.broadcastWin(payload)

# Update when winner detected (admin-only)
@app.put("/games/{game_id}/winner")
def update_game_winner(game_id: int, winner: str, role=Depends(verify_device_auth), db=Depends(get_db)):
    if role != "admin":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    try:
        db.update_game_winner(game_id, winner)
        return {"message": "Game winner updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get('/key')
def get_token():
    payload = {
        'data': 'junk',
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=4),
    }
    token = '123'
    return {'token': token} 

@app.get("/script.js")
async def get():
    with open('./script.js', 'r', encoding='utf-8') as f:
        script = f.read()
    return HTMLResponse(script)

@app.get("/{full_path:path}")
async def get():
    with open('./index.html', 'r', encoding='utf-8') as f:
        html = f.read()
    return HTMLResponse(html)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


