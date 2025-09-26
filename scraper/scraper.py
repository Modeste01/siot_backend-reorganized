import os
from fake_useragent import UserAgent  # Library to generate random user agents
from bs4 import BeautifulSoup, SoupStrainer  # For parsing and navigating HTML
import re  # Regular expressions
import time  # Time-related functions
from collections import defaultdict

import datetime  # Date and time handling
from datetime import date, timedelta, datetime, timezone
from datetime import time as datetimetime
import json  # JSON parsing
from dotmap import DotMap  # Easy access to nested dictionary attributes
from typing import Type
import schedule
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import threading
from queue import Queue, Empty
import argparse 
import pandas as pd
import gc
import requests
import sqlite3

import plugins
from db import Database

# Mapping of sports names to their NCAA sport codes
# TODO: Since basketball season is starting, ensure codes for those sports are included

TEAMS_CGI_MAPPING = {
    'Utah St.': 'usu',
    'Washington St.': 'wsu',
    'SFA': 'debugschool'
}

SPORTS_CODE = {
    'Volleyball (W)': 'WVB',
    'Soccer (W)': 'WSO',
    'Soccer (M)': 'MSO',
    'Football': 'MFB',
    'Basketball (M)': 'MBB',
    'Basketball (W)': 'WBB',
    'Baseball': 'MBA',
}

PLUGINS = {
    'Volleyball (W)': plugins.Volleyball,
    'Basketball (M)': plugins.MensBasketball,
    'Basketball (W)': plugins.WomensBasketball,
    'Football': plugins.Football,
    'Baseball': plugins.Baseball,
    'Soccer (W)': plugins.WomensSoccer,
    'Soccer (M)': plugins.MensSoccer,
}

def compare_dicts_excluding_key(dict1, dict2, excluded_key):

    if (dict2 == '' or dict1 == '') and dict1 != dict2:
        return False

    filtered_dict1 = {k: v for k, v in dict1.items() if k != excluded_key}
    filtered_dict2 = {k: v for k, v in dict2.items() if k != excluded_key}
    return filtered_dict1 == filtered_dict2

class WebGetter:
    def __init__(self, url):
        pass

    def restart(self, url):
        pass

    def query(self):
        pass

    def quit(self):
        pass
    

class WebPlayback(WebGetter):
    """
    The derived class that is used for debug and just prints out the parsed results
    """
    def __init__(self, url, date):
        super().__init__(url)
        # Determine the sport type from the URL
        self.sport = next((sport for sport, code in SPORTS_CODE.items() if code in url), None)
        # Use the sport type and a date to read the parquet file from the recordings directory
        # The code to save was: self.dataframes[sport].to_parquet(f'recordings/{sport}_{datetime.now().strftime("%Y-%m-%d")}.parquet', index=False, compression='zstd')
        self.df = pd.read_parquet(f'recordings/{self.sport}_{date.strftime("%Y-%m-%d")}.parquet')
        self.index = 0

    def query(self):
        if self.index < len(self.df):
            self.index += 1
            return True, self.df.iloc[self.index - 1]['html_text']
        else:
            return False, ''
        
    def restart(self, url=None):

        print("RESTARTING PLAYBACK DOES NOTHING")

    def quit(self):
        pass
        

class WebGrabber(WebGetter):
    """
    Event-driven Web grabber using a DOM MutationObserver. It installs an observer in the page
    and optionally blocks until a change occurs, avoiding fixed-interval polling.
    """

    def __init__(self, url):
        self.url = url
        # Optional: event-driven change detection
        self.dom_wait = os.getenv('DOM_WAIT', '0') in ('1', 'true', 'True')
        try:
            self.dom_wait_timeout = float(os.getenv('DOM_WAIT_TIMEOUT', '10'))
        except Exception:
            self.dom_wait_timeout = 10.0
        # Observer scope: 'body' (default, broad) or 'contest' (narrow)
        self.observe_scope = os.getenv('OBSERVE_SCOPE', 'body').lower()
        self.driver = self._create_driver()
        self.last_page_source = ''
        self._last_change_counter = 0
        self._first_emit = True

    def _create_driver(self):
        ua = UserAgent()
        options = Options()
        options.add_argument(f"user-agent={ua.random}")
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=options)
        driver.get(self.url)
        print(f"DRIVER CREATED FOR URL: {self.url}")
        self._install_dom_observer(driver)
        return driver

    def _install_dom_observer(self, drv):
        """Install MutationObserver; scope can be 'body' or 'contest'."""
        try:
            scope = self.observe_scope if self.observe_scope in ('body', 'contest') else 'body'
            res = drv.execute_script(
                """
                (function(scope){
                    if (!window.__siot_change_counter) { window.__siot_change_counter = 0; }
                    function setupOn(target, scopeName){
                        try{
                            if (window.__siot_obs) { try{ window.__siot_obs.disconnect(); }catch(e){} }
                            var obs = new MutationObserver(function(){ window.__siot_change_counter++; });
                            obs.observe(target, {subtree:true, childList:true, characterData:true, attributes:true});
                            window.__siot_obs = obs;
                            window.__siot_scope = scopeName;
                            return true;
                        }catch(e){ return false; }
                    }
                    if (scope === 'contest'){
                        var row = document.querySelector('tr[id^="contest_"]');
                        if (row){
                            var target = row.closest('div.col-md-auto.p-0') || row.closest('div.card') || row.closest('table') || row.parentElement || row;
                            setupOn(target, 'contest');
                        } else {
                            setupOn(document.body || document.documentElement, 'body');
                        }
                    } else {
                        setupOn(document.body || document.documentElement, 'body');
                    }
                    return [window.__siot_change_counter||0, window.__siot_scope||''];
                })(arguments[0]);
                """,
                scope
            )
            if isinstance(res, (list, tuple)) and len(res) >= 1:
                self._last_change_counter = int(res[0]) if res[0] is not None else 0
            else:
                self._last_change_counter = int(res) if res is not None else 0
        except Exception:
            self._last_change_counter = 0

    def restart(self, url=None):
        # Reuse the existing driver whenever possible; only recreate on failure
        if url is not None:
            self.url = url
        try:
            if getattr(self, 'driver', None) is None:
                self.driver = self._create_driver()
            else:
                if url is not None:
                    self.driver.get(self.url)
                else:
                    self.driver.refresh()
                # Reinstall observer after navigation/refresh
                self._install_dom_observer(self.driver)
                # Reset counters to force initial emit
                self._first_emit = True
        except Exception as ex:
            print(f"Driver restart encountered error, recreating: {ex}")
            try:
                self.quit()
            except Exception:
                pass
            self.driver = self._create_driver()

    def query(self):
        # Emit an initial snapshot without waiting, so pages with no live mutations still record once
        if self._first_emit:
            current_page_source = self.driver.page_source
            self.last_page_source = current_page_source
            self._first_emit = False
            return True, current_page_source

        # If using contest scope, try to re-scope when contest rows appear later
        if self.observe_scope == 'contest':
            try:
                self.driver.execute_script(
                    """
                    (function(){
                        var row = document.querySelector('tr[id^="contest_"]');
                        if (row && window.__siot_scope !== 'contest'){
                            if (window.__siot_obs) { try{ window.__siot_obs.disconnect(); }catch(e){} }
                            var target = row.closest('div.col-md-auto.p-0') || row.closest('div.card') || row.closest('table') || row.parentElement || row;
                            try {
                                var obs = new MutationObserver(function(){ window.__siot_change_counter++; });
                                obs.observe(target, {subtree:true, childList:true, characterData:true, attributes:true});
                                window.__siot_obs = obs; window.__siot_scope = 'contest';
                            } catch(e) {}
                        }
                    })();
                    """
                )
            except Exception:
                pass

        if self.dom_wait:
            try:
                WebDriverWait(self.driver, self.dom_wait_timeout, poll_frequency=0.25).until(
                    lambda d: (d.execute_script("return window.__siot_change_counter || 0") or 0) > self._last_change_counter
                )
            except TimeoutException:
                return False, self.last_page_source

        try:
            counter = self.driver.execute_script("return window.__siot_change_counter || 0") or 0
        except Exception:
            counter = 0

        if counter > self._last_change_counter:
            self._last_change_counter = counter
            current_page_source = self.driver.page_source
            if self.last_page_source != current_page_source:
                self.last_page_source = current_page_source
                return True, current_page_source
            else:
                return False, self.last_page_source
        else:
            return False, self.last_page_source

    def quit(self):
        if getattr(self, 'driver', None) is not None:
            try:
                self.driver.quit()
            finally:
                self.driver = None



class DatabasePutter:
    """
    Base class for the database putter. Currently there are two derived variants:
        (1) A debug class that just prints out the extracted values
        (2) One that inserts into the PostreSQL database
    """

    def __init__(self, config:DotMap):
        pass

    def insert_sport(self, sport):
        pass

    def insert_school(self, school, sport):
        pass

    def insert_game(self, game_info):
        pass


class PostgresDatabasePutter(DatabasePutter):
    """
    The derived class that puts the data into the PostgreSQL database
    """
    def __init__(self, config:DotMap):
        super().__init__(config)

        self.db = Database(
            config.database.dbname,
            config.database.user,
            config.database.password,
            config.database.host,
            config.database.port
        )
    
    def insert_sport(self, sport):
        self.db.insert_sport(sport)

    def insert_school(self, school, sport):
        self.db.insert_school(school, sport)

    def insert_game(self, game_info):
        self.db.insert_game(game_info)

class DebugPrintDatabasePutter(DatabasePutter):
    """
    The derived class that is used for debug and just prints out the parsed results
    """
    def __init__(self, config:DotMap):
        super().__init__(config)

    def insert_sport(self, sport):
        print(f"INSERT sport: {sport}")

    def insert_school(self, school, sport):
        print(f"INSERT school {school} with sport {sport}")

    def insert_game(self, game_info):
        # Pretty, fixed-width debug table that avoids wrapping by truncating long values
        try:
            total_width = int(os.getenv("DEBUG_TABLE_WIDTH", "100"))
            key_width = 18
            # Borders and spacing: | <key> | <val> |
            val_width = max(10, total_width - key_width - 5)

            def trunc(val: str) -> str:
                s = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
                if len(s) > val_width:
                    return s[: val_width - 3] + "..."
                return s

            def row(k: str, v) -> str:
                return f"| {k:<{key_width}}| {trunc(v):<{val_width}}|"

            def sep(char: str = '-') -> str:
                return "+" + char * (key_width + 2) + "+" + char * (val_width + 2) + "+"

            lines = []
            lines.append(sep('='))
            lines.append(row("Field", "Value"))
            lines.append(sep('='))

            # Common fields in desired order
            ordered = [
                ("Sport", game_info.get("sport")),
                ("Status", game_info.get("status")),
                ("Date", game_info.get("date")),
                ("Time (UTC)", game_info.get("time")),
                ("Home Team", game_info.get("home_team")),
                ("Away Team", game_info.get("away_team")),
                ("Score", game_info.get("score")),
                ("Winner", game_info.get("winner")),
                ("Attendance", game_info.get("attendance")),
                ("Game Link", game_info.get("game_link")),
            ]

            # Optional live fields
            if "current_period" in game_info:
                ordered.append(("Current Period", game_info.get("current_period")))
            if "current_clock" in game_info:
                ordered.append(("Current Clock", game_info.get("current_clock")))

            for k, v in ordered:
                if v is not None:
                    lines.append(row(k, v))

            # Sport-specific details (flattened JSON)
            if game_info.get("sport_details"):
                lines.append(sep())
                lines.append(row("Sport Details", json.dumps(game_info["sport_details"], ensure_ascii=False)))

            lines.append(sep('='))

            print("\n".join(lines))
        except Exception:
            # Fallback to simple print if formatting fails
            print(f"INSERT game info : {game_info}")

class APIDatabasePutter(DatabasePutter):
    def __init__(self, config:DotMap):
        self.url = config.api.url
        self.token = config.api.token

    def insert_sport(self, sport):
        data = {
            'name': sport
        }
        requests.post(f'{self.url}/sports', json=data, headers={'Authorization': f'Bearer {self.token}'})

    def insert_school(self, school, sport):
        data = {
            'name': school,
            'sport': sport
        }
        requests.post(f'{self.url}/schools', json=data, headers={'Authorization': f'Bearer {self.token}'})

    def insert_game(self, game_info):
        # TODO: This could be better. I don't know what all the field of game_info are, so and just printing out some of them
        requests.post(f'{self.url}/games', json=game_info, headers={'Authorization': f'Bearer {self.token}'})



def update_sport_status_json(school, sport, status_int):
    """
    Sends a POST request with a JSON payload to update a single sport's status.
    """
    url = "https://sports-iot.com/update_sports_debugjson.py"

    # JSON payload
    payload = {
        "school": school,
        "sport": sport,
        "status": status_int
    }

    # Headers matching the working curl command
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept": "application/json",
        # "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json",
        # "X-Requested-With": "XMLHttpRequest"  # Often used by servers to identify AJAX calls
    }

    try:
        print(f"Sending payload: {payload}")
        # Perform the POST request
        response = requests.post(url, json=payload, headers=headers)
        print(f"Response status code: {response.status_code}")
        response.raise_for_status()  # Raise an exception for HTTP errors
        print(f"Response body: {response.text}")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


class CGIDatabasePutter(DatabasePutter):
    """
    The derived class that is used for debug and just prints out the parsed results
    """
    team_list = []

    def __init__(self, config:DotMap):
        super().__init__(config)

        print('Config teams')
        for row in config.teams:
            print(row.name)
            self.team_list.append(row.name)

        # Setup idempotency cache (persists across runs)
        state_dir = os.path.join(os.path.dirname(__file__), 'state')
        os.makedirs(state_dir, exist_ok=True)
        self.cache_path = os.path.join(state_dir, 'cgi_notify.sqlite')
        self.cache = sqlite3.connect(self.cache_path, check_same_thread=False)
        self.cache.execute(
            """
            CREATE TABLE IF NOT EXISTS notified (
                day   TEXT NOT NULL,
                winner TEXT NOT NULL,
                sport  TEXT NOT NULL,
                PRIMARY KEY(day, winner, sport)
            );
            """
        )
        self.cache.commit()
        # Start background thread to clear previous day's winners at 8AM
        self._stop_clear_thread = False
        self._clear_thread = threading.Thread(target=self._clear_worker, daemon=True)
        self._clear_thread.start()

    def insert_sport(self, sport):
        # print(f"INSERT sport: {sport}")
        pass

    def insert_school(self, school, sport):
        # print(f"INSERT school {school} with sport {sport}")
        pass
        # self.team_list.append(school)

    def insert_game(self, game_info):
        # TODO: This could be better. I don't know what all the field of game_info are, so and just printing out some of them
        # print(f"INSERT game)info : {game_info}")

        if game_info['status'] == 'Final':
            winner_sport_pair = (game_info['winner'], game_info['sport'])
            # Only post for configured teams
            if game_info['winner'] in self.team_list and game_info['winner'] in list(TEAMS_CGI_MAPPING.keys()):
                # Derive sports day with 3AM boundary: use (now - 3h).date()
                now = datetime.now()
                pseudo_day = (now - timedelta(hours=3)).date().isoformat()

                try:
                    cur = self.cache.cursor()
                    cur.execute(
                        "INSERT OR IGNORE INTO notified(day, winner, sport) VALUES (?, ?, ?)",
                        (pseudo_day, game_info['winner'], game_info['sport'])
                    )
                    self.cache.commit()
                    if cur.rowcount == 1:
                        # First time today -> send
                        print(f"Posting winner once for {game_info['winner']} ({game_info['sport']}) on {pseudo_day}")
                        update_sport_status_json(TEAMS_CGI_MAPPING[game_info['winner']], game_info['sport'], 1)
                    else:
                        # Already posted today; skip
                        print(f"Already posted today for {game_info['winner']} ({game_info['sport']}); skipping")
                except Exception as e:
                    print(f"Idempotency cache error: {e}")
    
    # crossed_3am_boundary no longer needed; persistent cache controls once-per-day posting

    def clear_game(self, winner: str, sport: str):
        try:
            if winner not in TEAMS_CGI_MAPPING:
                print(f"No CGI mapping for {winner}; skipping clear.")
                return False
            print(f"Clearing status for {winner} ({sport}) -> 0")
            resp = update_sport_status_json(TEAMS_CGI_MAPPING[winner], sport, 0)
            return resp is not None
        except Exception as e:
            print(f"Clear game error for {winner} ({sport}): {e}")
            return False

    def _next_8am(self, now: datetime) -> datetime:
        target = datetime.combine(now.date(), datetimetime(8, 0))
        if now >= target:
            target = target + timedelta(days=1)
        return target

    def _clear_worker(self):
        while not getattr(self, '_stop_clear_thread', False):
            now = datetime.now()
            nxt = self._next_8am(now)
            sleep_s = max(0.0, (nxt - now).total_seconds())
            try:
                # Sleep until 8AM
                time.sleep(sleep_s)
            except Exception:
                pass

            if getattr(self, '_stop_clear_thread', False):
                break

            try:
                # At 8AM, clear previous sports-day winners
                ref = datetime.now()
                target_day = (ref - timedelta(hours=3) - timedelta(days=1)).date().isoformat()
                cur = self.cache.cursor()
                cur.execute("SELECT winner, sport FROM notified WHERE day = ?", (target_day,))
                rows = cur.fetchall()

                for winner, sport in rows:
                    print(f"8AM clear worker processing {winner} ({sport}) for day {target_day}")
                    ok = self.clear_game(winner, sport)
                    if ok:
                        try:
                            cur2 = self.cache.cursor()
                            cur2.execute("DELETE FROM notified WHERE day = ? AND winner = ? AND sport = ?", (target_day, winner, sport))
                            self.cache.commit()
                        except Exception as de:
                            print(f"Failed to delete cache row for {winner}/{sport}: {de}")
                    else:
                        print(f"Skipping delete for {winner}/{sport}; clear failed.")
            except Exception as e:
                print(f"8AM clear worker error: {e}")

class Parser:
    @staticmethod
    def extract_school_column(soup, school_name):
        """Extracts the specific <div class="col-md-auto p-0"> column containing the specified school.
        
        Args:
            html_content (str): The HTML content to parse.
            school_name (str): The name of the school to search for.

        Returns:
            str: The HTML of the encapsulating <div class="col-md-auto p-0"> column with the team,
                 or a message if not found.
        """

        try:
            # Normalize function to compare school names robustly
            def _norm(s: str) -> str:
                if not isinstance(s, str):
                    return ""
                return re.sub(r"[^a-z0-9]", "", s.lower())

            target = _norm(school_name)

            # Search contest rows for a matching school by img alt or link text
            for row in soup.find_all("tr", id=re.compile(r"^contest_\d+")):
                # Candidate strings to check in this row
                texts = []
                img = row.find("img", alt=True)
                if img:
                    texts.append(img.get("alt", ""))
                a = row.find("a")
                if a:
                    texts.append(a.get_text(strip=True))

                if any(_norm(t).find(target) != -1 for t in texts):
                    # Found the row; return the enclosing column container
                    column_div = row.find_parent(
                        "div",
                        class_=lambda c: isinstance(c, list) and ("col-md-auto" in c and "p-0" in c),
                    )
                    if column_div is None:
                        # Fallback to card container
                        column_div = row.find_parent("div", class_="card")
                    return column_div, False

            # Not found
            return None, False
        except Exception as ex:
            print(ex)
            return (None, True)

    @staticmethod
    def parse_sport_event(soup, sport):
        try:
            return PLUGINS[sport](soup).game_info, False
        except Exception as ex:
            print(ex)
            return None, True
        


class Controller:
    """
    The Controller will be they entry class of the program. It initilizes
    all the sports/WebGrabbers, gets their data, pipes it into the parser,
    then to the database. 
    """
    def __init__(self, config_file, webgrabber:Type[WebGetter], parser:Type[Parser], dbputter:Type[DatabasePutter], playback_date=None):
        if webgrabber == WebPlayback and playback_date is None:
            raise ValueError('playback_date must be provided when using WebPlayback')

        self.config = DotMap()
        with open(config_file) as json_data_file:
            self.config = DotMap(json.load(json_data_file))        

        self.webgrabber = webgrabber
        self.parser = parser
        self.dbputter = dbputter(self.config)

        self.sports = defaultdict(set)
        for team in self.config.teams:
            for sport in team.sports:
                self.sports[sport].add(team.name)
        
        for key, value in self.sports.items():
            self.sports[key] = list(value)

        self.grabbers = {}
    # Track consecutive parse failures per sport to avoid aggressive restarts
        self.parse_failures = defaultdict(int)
        for sport in self.sports:
            if webgrabber == WebPlayback:
                self.grabbers[sport] = webgrabber(Controller.build_url(sport), playback_date)
            elif webgrabber == WebGrabber:
                self.grabbers[sport] = webgrabber(Controller.build_url(sport))
            else:
                raise ValueError("webgrabber must be of type WebGrabber or WebPlayback")
            # self.grabbers[sport] = self.webgrabber(Controller.build_url(sport))

        # do db stuff
        for sport in self.sports.keys():
            self.dbputter.insert_sport(sport)

        # Schedule the task for 2 AM daily
        schedule.every().day.at("02:00").do(self.restart_grabbers)

        # If using event-driven WebGrabber, set up queue and worker threads
        self.queue = None
        self.threads = {}
        if webgrabber == WebGrabber:
            self.queue = Queue()
            for sport in self.sports:
                t = threading.Thread(target=self._worker_loop, args=(sport,), daemon=True)
                t.start()
                self.threads[sport] = t
        else:
            # Poll interval from config, default to 2s
            self.interval_seconds = 2
            try:
                if self.config.timing and self.config.timing.interval_seconds is not None:
                    self.interval_seconds = int(self.config.timing.interval_seconds)
            except Exception:
                pass

    @staticmethod
    def build_url(sport):
        gameday = date.today()
        code = SPORTS_CODE[sport]
        return (
            f'https://stats.ncaa.org/contests/livestream_scoreboards?'
            f'sport_code={code}&game_date={gameday.month}%2F{gameday.day}%2F{gameday.year}'
        )

    def restart_grabbers(self):
        print('Restarting grabbers on daily schedule')
        for sport in self.sports:
            new_url = Controller.build_url(sport)
            self.grabbers[sport].restart(url=new_url)
        
    def run(self):
        previous_game_info = defaultdict(lambda: "")

        # Event-driven path
        if isinstance(self.grabbers[next(iter(self.grabbers))], WebGrabber) and self.queue is not None:
            while True:
                drained = 0
                # Drain queue without blocking
                while True:
                    try:
                        sport, html = self.queue.get_nowait()
                    except Empty:
                        break
                    drained += 1

                    print(f"Observed a change for {sport}")

                    soup = BeautifulSoup(html, 'lxml')
                    has_contest_rows = soup.find('tr', id=re.compile(r'^contest_')) is not None

                    if has_contest_rows:
                        self.parse_failures[sport] = 0
                        for team in self.sports[sport]:
                            school_column_soup, hadErr = self.parser.extract_school_column(soup, team)
                            if hadErr:
                                print("ERROR EXTRACTING SCHOOL COLUMN")

                            if school_column_soup is not None:
                                
                                game_info, hadErr = self.parser.parse_sport_event(school_column_soup, sport)
                                if hadErr:
                                    print("ERROR PARSING SPORT EVENT")

                                game_info["sport"] = sport
                                key = f"{sport}:{team}"
                                if compare_dicts_excluding_key(game_info, previous_game_info[key], 'time') == False:
                                    if previous_game_info[key] != '' and isinstance(previous_game_info[key], dict) and previous_game_info[key].get('status') != 'Final' and game_info.get('status') == 'Final':
                                        print(f'**** GAME WENT FINAL - {sport}:{team} ****')

                                    print(f"Found a change for team: {team} in sport: {sport}")
                                    dt = game_info.get("time")
                                    if isinstance(dt, datetime):
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        dt = dt.astimezone(timezone.utc)
                                        game_info["time"] = str(dt)
                                    previous_game_info[key] = game_info
                                    self.dbputter.insert_school(game_info["home_team"], sport)
                                    self.dbputter.insert_school(game_info["away_team"], sport)
                                    self.dbputter.insert_game(game_info)
                    else:
                        self.parse_failures[sport] += 1
                        if self.parse_failures[sport] >= 10:
                            print(f"Restarting grabber for {sport} after {self.parse_failures[sport]} consecutive parse misses (no contest rows found)")
                            self.grabbers[sport].restart()
                            self.parse_failures[sport] = 0
                        else:
                            print(f"Parse miss for {sport} (no contest rows yet); attempt {self.parse_failures[sport]}/10")

                schedule.run_pending()
                # Light sleep; workers block on DOM mutations when enabled
                if any(getattr(g, 'dom_wait', False) for g in self.grabbers.values() if isinstance(g, WebGrabber)):
                    time.sleep(0.05)
                else:
                    time.sleep(0.5)
                gc.collect()
        else:
            # Polling path (WebPlayback or legacy)
            times_queried = 0
            go = True
            while go:
                for sport in self.sports:
                    success, html = self.grabbers[sport].query()
                    if self.webgrabber == WebPlayback and not success:
                        print("#### Done ####")
                        go = False

                    soup = BeautifulSoup(html, 'lxml')
                    has_contest_rows = soup.find('tr', id=re.compile(r'^contest_')) is not None

                    if has_contest_rows:
                        self.parse_failures[sport] = 0
                        for team in self.sports[sport]:
                            school_column_soup, hadErr = self.parser.extract_school_column(soup, team)
                            if hadErr:
                                print("ERROR EXTRACTING SCHOOL COLUMN")
                            if school_column_soup is not None:
                                game_info, hadErr = self.parser.parse_sport_event(school_column_soup, sport)
                                if hadErr:
                                    print("ERROR PARSING SPORT EVENT")
                                game_info["sport"] = sport
                                key = f"{sport}:{team}"
                                if compare_dicts_excluding_key(game_info, previous_game_info[key], 'time') == False:
                                    if previous_game_info[key] != '' and isinstance(previous_game_info[key], dict) and previous_game_info[key].get('status') != 'Final' and game_info.get('status') == 'Final':
                                        print(f'**** GAME WENT FINAL - {sport}:{team} ****')
                                    dt = game_info.get("time")
                                    if isinstance(dt, datetime):
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        dt = dt.astimezone(timezone.utc)
                                        game_info["time"] = str(dt)
                                    previous_game_info[key] = game_info
                                    self.dbputter.insert_school(game_info["home_team"], sport)
                                    self.dbputter.insert_school(game_info["away_team"], sport)
                                    self.dbputter.insert_game(game_info)
                    else:
                        self.parse_failures[sport] += 1
                        if self.parse_failures[sport] >= 10:
                            print(f"Restarting grabber for {sport} after {self.parse_failures[sport]} consecutive parse misses (no contest rows found)")
                            self.grabbers[sport].restart()
                            self.parse_failures[sport] = 0
                        else:
                            print(f"Parse miss for {sport} (no contest rows yet); attempt {self.parse_failures[sport]}/10")

                schedule.run_pending()
                times_queried += 1
                time.sleep(max(self.interval_seconds, 0))
                gc.collect()

    def _worker_loop(self, sport: str):
        g = self.grabbers[sport]
        while True:
            success, html = g.query()
            if success:
                self.queue.put((sport, html))
            else:
                # In polling mode (dom_wait disabled), avoid tight spin
                if not getattr(g, 'dom_wait', False):
                    time.sleep(0.25)


def scraper_main(config_path: str, mode = 'no_db'):
    # Choose DB putter based on flags
    if mode == 'no_db':
        db_putter_cls = DebugPrintDatabasePutter
    elif mode == 'cgi':
        db_putter_cls = CGIDatabasePutter
    elif mode == 'api':
        db_putter_cls = APIDatabasePutter
    else:
        db_putter_cls = PostgresDatabasePutter

    controller = Controller(config_path, WebGrabber, Parser, db_putter_cls)
    controller.run()


if __name__ == '__main__':
    config_path = os.getenv('CONFIG_PATH', os.path.join(os.path.dirname(__file__), 'config_debug.json'))
    use_api = os.getenv('USE_API', '0') in ('1', 'true', 'True')
    no_db = os.getenv('NO_DB', '0') in ('1', 'true', 'True')
    use_cgi = os.getenv('USE_CGI', '0') in ('1', 'true', 'True')

    mode = 'no_db' if no_db else 'api' if use_api else 'cgi' if use_cgi else 'db'
    print(f"Starting scraper in mode: {mode}")

    scraper_main(config_path=config_path, mode=mode)
