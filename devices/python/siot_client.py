import asyncio
import json
import os
import signal
import sys
import uuid
from dataclasses import dataclass
from typing import Callable, List, Optional, Awaitable

from websockets.legacy.client import connect, WebSocketClientProtocol

# Contract
# - Connect to ws://<host>:<port>/ws/{uid}
# - Send first message as JSON: {"uid": str, "school": str, "sports": [str, ...]}
# - Use Authorization: Bearer <token> header
# - Server sends an init snapshot: {"init": true, "games": [ {game}, ... ]}
# - Then individual game updates as JSON objects, including winner when final


@dataclass
class GameInfo:
    id: int = -1
    sport: str = ""
    home_team: str = ""
    away_team: str = ""
    winner: str = ""
    homeScore: Optional[int] = None
    awayScore: Optional[int] = None
    date: str = ""
    time: str = ""


InitCallback = Callable[[int], Awaitable[None]] | Callable[[int], None]
UpdateCallback = Callable[[GameInfo], Awaitable[None]] | Callable[[GameInfo], None]
WinCallback = Callable[[GameInfo], Awaitable[None]] | Callable[[GameInfo], None]


class SIOTPythonClient:
    def __init__(
        self,
        url: str,
        uid: str,
        school: str,
        sports: List[str],
        token: str,
        on_init: Optional[InitCallback] = None,
        on_update: Optional[UpdateCallback] = None,
        on_win: Optional[WinCallback] = None,
        reconnect_delay: float = 5.0,
    ) -> None:
        self.url = url.rstrip("/")
        self.uid = uid
        self.school = school
        self.sports = sports
        self.token = token
        self.on_init = on_init
        self.on_update = on_update
        self.on_win = on_win
        self.reconnect_delay = reconnect_delay
        self._stop = asyncio.Event()
        self._ws: Optional[WebSocketClientProtocol] = None

    async def start(self):
        while not self._stop.is_set():
            try:
                # Use legacy interface so extra_headers works consistently
                extra_headers = [("Authorization", f"Bearer {self.token}")] if self.token else None
                print(f"Extra headers: {extra_headers}", file=sys.stderr)
                async with connect(
                    self.url,
                    extra_headers=extra_headers,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    self._ws = ws
                    await self._send_registration()
                    await self._recv_loop(ws)       
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"WS error: {e}")
                await asyncio.sleep(self.reconnect_delay)
        await self._close_ws()

    async def stop(self):
        self._stop.set()
        await self._close_ws()

    async def _close_ws(self):
        try:
            if self._ws and not self._ws.closed:
                await self._ws.close()
        except Exception:
            pass
        self._ws = None

    async def _send_registration(self):
        payload = {"uid": self.uid, "school": self.school, "sports": self.sports}
        await self._ws.send(json.dumps(payload))

    async def _recv_loop(self, ws: WebSocketClientProtocol):
        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                # Ignore non-JSON text (e.g., simple pongs)
                continue

            if isinstance(data, dict) and data.get("init") is True and isinstance(data.get("games"), list):
                count = 0
                for g in data["games"]:
                    gi = self._parse_game(g)
                    count += 1
                    if self.on_update:
                        await _maybe_await(self.on_update(gi))
                if self.on_init:
                    await _maybe_await(self.on_init(count))
                continue

            if isinstance(data, dict):
                gi = self._parse_game(data)
                if self.on_update:
                    await _maybe_await(self.on_update(gi))
                if gi.winner and gi.winner == self.school and self.on_win:
                    await _maybe_await(self.on_win(gi))

    

    def _parse_game(self, obj: dict) -> GameInfo:
        gi = GameInfo(
            id=int(obj.get("id", -1)) if obj.get("id") is not None else -1,
            sport=str(obj.get("sport", "") or ""),
            home_team=str(obj.get("home_team", "") or ""),
            away_team=str(obj.get("away_team", "") or ""),
            winner=str(obj.get("winner", "") or ""),
            date=str(obj.get("date", "") or ""),
            time=str(obj.get("time", "") or ""),
        )
        score = obj.get("score")
        if isinstance(score, dict):
            gi.homeScore = _safe_int(score.get("home"))
            gi.awayScore = _safe_int(score.get("away"))
        return gi


def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


async def _maybe_await(result):
    if asyncio.iscoroutine(result):
        return await result
    return result


# Simple runnable example
async def main():
    # Read configuration from env or defaults
    host = os.getenv("SIOT_HOST", "localhost")
    port = int(os.getenv("SIOT_PORT", "8000"))
    raw_uid = os.getenv("SIOT_UID")
    if raw_uid and raw_uid.isdigit():
        uid = raw_uid
    else:
        # Server expects /ws/{user_id} with an integer; generate a 6-digit numeric id by default
        uid = str(int.from_bytes(os.urandom(3), 'big') % 900000 + 100000)
    token = os.getenv("SIOT_TOKEN", "abc123")  # must match API AUTH_TOKEN
    school = os.getenv("SIOT_SCHOOL", "Montana")
    sports = [s for s in os.getenv("SIOT_SPORTS", "Soccer (W)").split(",") if s]

    ws_url = f"ws://{host}:{port}/ws/{uid}"

    async def on_init(count: int):
        print(f"Init received: {count} games")

    async def on_update(g: GameInfo):
        print(f"Update: [{g.sport}] {g.home_team} vs {g.away_team} winner={g.winner or '-'} time={g.time}")

    async def on_win(g: GameInfo):
        print(f"WIN! {g.winner} in {g.sport} for game {g.id}")

    client = SIOTPythonClient(
        url=ws_url,
        uid=uid,
        school=school,
        sports=sports,
        token=token,
        on_init=on_init,
        on_update=on_update,
        on_win=on_win,
    )

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_sig():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_sig)
        except NotImplementedError:
            pass

    runner = asyncio.create_task(client.start())
    await stop_event.wait()
    await client.stop()
    await runner


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
