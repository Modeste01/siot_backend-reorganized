# SIOT Python WebSocket Client

A Python client that mirrors the Arduino `SIOTClient` behavior. It:

- Connects to your API's websocket endpoint `ws://<host>:<port>/ws/{uid}`
- Sends a registration payload on connect: `{ "uid": str, "school": str, "sports": [str, ...] }`
- Includes `Authorization: Bearer <token>` header
- Handles the initial snapshot `{ init: true, games: [...] }` and subsequent single-game updates
- Exposes callback hooks: onInit(count), onUpdate(GameInfo), onWin(GameInfo)

## Install

Create/activate a virtualenv (optional) and install deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r devices/python/requirements.txt
```

## Run

Configure via environment variables (defaults shown):

- `SIOT_HOST=localhost`
- `SIOT_PORT=8000`
- `SIOT_UID=<numeric id>`  # IMPORTANT: current API expects an integer user_id in /ws/{user_id}
- `SIOT_TOKEN=abc123`  # must match API `AUTH_TOKEN`
- `SIOT_SCHOOL=BYU`
- `SIOT_SPORTS=Basketball,Football`

Then run:

```bash
python devices/python/siot_client.py
```

You'll see logs for init, updates, and wins.

## Library Usage

```python
import asyncio
from devices.python.siot_client import SIOTPythonClient, GameInfo

async def on_init(count:int):
    print("init", count)

async def on_update(g: GameInfo):
    print("update", g)

async def on_win(g: GameInfo):
    print("win", g)

client = SIOTPythonClient(
    url="ws://localhost:8000/ws/12345",
    uid="12345",  # use a numeric uid unless the API is updated to accept strings
    school="BYU",
    sports=["Basketball","Football"],
    token="abc123",
    on_init=on_init,
    on_update=on_update,
    on_win=on_win,
)

asyncio.run(client.start())
```
