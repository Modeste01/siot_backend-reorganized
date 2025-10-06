import asyncio
import json
import os
import signal
from datetime import datetime, timedelta, timezone
from typing import Dict, Set

import requests

from siot_client import SIOTPythonClient, GameInfo


# Default team-to-CGI code mapping (can be overridden by LEGACY_TEAM_MAP_JSON)
TEAMS_CGI_MAPPING = {
    'Utah St.': 'usu',
    'Washington St.': 'wsu',
    'SFA': 'debugschool',
}


def load_team_map() -> Dict[str, str]:
    raw = os.getenv('LEGACY_TEAM_MAP_JSON')
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except Exception:
            pass
    return TEAMS_CGI_MAPPING.copy()


def update_sport_status_json(school_code: str, sport: str, status_int: int, url: str | None = None) -> bool:
    """Send a POST to the legacy CGI to update status for a school/sport.

    Args:
        school_code: short code expected by CGI (e.g., 'usu', 'wsu')
        sport: sport name string
        status_int: 1 to set, 0 to clear
        url: override CGI endpoint; defaults to production URL
    Returns:
        True on HTTP 2xx, else False
    """
    cgi_url = url or os.getenv('LEGACY_CGI_URL', 'https://sports-iot.com/update_sports_debugjson.py')
    payload = {"school": school_code, "sport": sport, "status": int(status_int)}
    headers = {
        "User-Agent": "SIOT-Legacy-Notifier/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(cgi_url, json=payload, headers=headers, timeout=10)
        print(f"CGI POST {cgi_url} -> {resp.status_code} | payload={payload}")
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"CGI POST failed: {e}")
        return False


class LegacyNotifier:
    def __init__(self, school: str, sports: list[str], team_map: Dict[str, str]):
        self.school = school
        self.sports = sports
        self.team_map = team_map
        # In-memory idempotency for this process: once per (day,winner,sport)
        self._posted: Set[str] = set()

    def _sports_day_key(self, winner: str, sport: str) -> str:
        # 3AM boundary like scraper: use (now - 3h).date()
        now = datetime.now(timezone.utc)
        pseudo_day = (now - timedelta(hours=3)).date().isoformat()
        return f"{pseudo_day}|{winner}|{sport}"

    async def on_win(self, g: GameInfo):
        # Only act when configured school wins in a monitored sport
        if g.winner != self.school or g.sport not in self.sports:
            return
        key = self._sports_day_key(g.winner, g.sport)
        if key in self._posted:
            print(f"Already posted today for {g.winner} ({g.sport}); skipping")
            return
        code = self.team_map.get(g.winner)
        if not code:
            print(f"No CGI mapping for winner '{g.winner}'; skipping")
            return
        ok = update_sport_status_json(code, g.sport, 1)
        if ok:
            self._posted.add(key)


async def main():
    host = os.getenv("SIOT_HOST", "localhost")
    port = int(os.getenv("SIOT_PORT", "8000"))
    uid = os.getenv("SIOT_UID") or os.getenv("HOSTNAME") or os.getenv("USER") or "legacy-notifier"
    token = os.getenv("SIOT_TOKEN", "abc123")
    school = os.getenv("SIOT_SCHOOL", "Utah St.")
    # sports = [s.strip() for s in os.getenv("SIOT_SPORTS", "Soccer (W), Volleyball (W), Football").split(",") if s.strip()]
    sports = [s.strip() for s in os.getenv("SIOT_SPORTS", "Soccer (W), Volleyball (W)").split(",") if s.strip()]

    ws_url = f"ws://{host}:{port}/ws/{uid}"
    team_map = load_team_map()
    notifier = LegacyNotifier(school=school, sports=sports, team_map=team_map)

    async def on_init(count: int):
        print(f"[LegacyNotifier] init: {count} games")

    async def on_update(g: GameInfo):
        # Optional: log updates
        pass

    client = SIOTPythonClient(
        url=ws_url,
        uid=str(uid),
        school=school,
        sports=sports,
        token=token,
        on_init=on_init,
        on_update=on_update,
        on_win=notifier.on_win,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    task = asyncio.create_task(client.start())
    await stop_event.wait()
    await client.stop()
    await task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
