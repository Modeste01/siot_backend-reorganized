import os
from fake_useragent import UserAgent  # Library to generate random user agents
from bs4 import BeautifulSoup  # For parsing and navigating HTML
import re  # Regular expressions
import time  # Time-related functions
from collections import defaultdict
import threading
from queue import Queue, Empty

import datetime  # Date and time handling
from datetime import date, timedelta, datetime
from datetime import time as datetimetime
import json  # JSON parsing
from dotmap import DotMap  # Easy access to nested dictionary attributes
from typing import Type
import schedule
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

import sqlite3
import zlib


class WebGrabber:
    """
    Grabs HTML from the web. Updates to only create a driver once and get the page once, then
    continuously just look at the page_source to see if it has changed
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
        # options.add_argument("--disable-gpu")  # Disable GPU acceleration
        # options.add_argument("--window-size=1920,1080")  # Set a window size (necessary for some cases)
        options.add_argument("--no-sandbox")  # Helps in containerized environments (because contianer use root user)
        # options.add_argument("--disable-blink-features=AutomationControlled")
        # options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # options.add_experimental_option("useAutomationExtension", False)
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

        if url is not None:
             self.url = url

        self.quit()
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
        self.driver.quit()

SPORTS_CODE = {
    'Volleyball (W)': 'WVB',
    'Soccer (W)': 'WSO',
    'Football': 'MFB',
    'Basketball (M)': 'MBB',
    'Basketball (W)': 'WBB'
}

class Recorder:
    # Storage now uses SQLite (WAL) with batched inserts for fast appends

    """
    The Recorder class will be responsible for querying the NCAA website for the current day's games and saving every query to a CSV file.
    This can then later be used for playback.
    """
    def __init__(self, config_file, webgrabber:Type[WebGrabber]):

        self.config = DotMap()
        with open(config_file) as json_data_file:
            self.config = DotMap(json.load(json_data_file))

        self.webgrabber = webgrabber
        
        self.sports = defaultdict(set)
        for team in self.config.teams:
            for sport in team.sports:
                self.sports[sport].add(team.name)
        
        for key, value in self.sports.items():
            self.sports[key] = list(value)

        self.grabbers = {}
        for sport in self.sports:
            self.grabbers[sport] = self.webgrabber(Recorder.build_url(sport))

        # Queue and worker threads per sport for event-driven capture
        self.queue: Queue = Queue()
        self.threads = {}
        for sport in self.sports:
            t = threading.Thread(target=self._worker_loop, args=(sport,), daemon=True)
            t.start()
            self.threads[sport] = t
        
        # Create a recordings folder if it doesn't exist
        if not os.path.exists('recordings'):
            os.makedirs('recordings')

        # Initialize SQLite database (single file) with WAL for concurrent reads and fast appends
        db_path = os.path.join('recordings', 'recordings.sqlite')
        self.conn = sqlite3.connect(db_path)
        # Recommended pragmas for append-heavy workloads
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA temp_store=MEMORY;")
            # Encourage regular auto-checkpointing to bound WAL growth
            self.conn.execute("PRAGMA wal_autocheckpoint=1000;")
        except Exception:
            pass
        # Create table and index if they don't exist
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recordings (
                sport TEXT NOT NULL,
                ts    TEXT NOT NULL,
                html  TEXT NOT NULL
            );
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recordings_sport_ts ON recordings(sport, ts);"
        )
        self.conn.commit()

        # Compression settings
        self.compress_html = os.getenv('COMPRESS_HTML', '1') in ('1', 'true', 'True')
        try:
            self.compress_level = int(os.getenv('COMPRESS_LEVEL', '6'))
        except Exception:
            self.compress_level = 6


        # Schedule the task for 2 AM daily
        schedule.every().day.at("02:00").do(self.restart_grabbers)
        # Periodic WAL checkpoint to keep -wal file small during long runs
        schedule.every().minute.do(self._checkpoint_wal)
        

    def run(self):
        while True:
            # Drain any updates from workers
            drained = 0
            batch = []  # collect rows for a single batched insert
            while True:
                try:
                    sport, ts, html = self.queue.get_nowait()
                except Empty:
                    break
                drained += 1
                print(f"* QUERYING SPORT : {sport}")
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                payload = html
                if self.compress_html:
                    try:
                        payload = sqlite3.Binary(zlib.compress(html.encode('utf-8'), self.compress_level))
                    except Exception as e:
                        try:
                            print(f"Compression failed, storing plain text: {e}")
                        except Exception:
                            pass
                        payload = html
                batch.append((sport, ts_str, payload))

            # Perform one executemany + commit per drain cycle for speed
            if batch:
                try:
                    cur = self.conn.cursor()
                    cur.executemany(
                        "INSERT INTO recordings (sport, ts, html) VALUES (?, ?, ?)",
                        batch,
                    )
                    self.conn.commit()
                except Exception as e:
                    # Best-effort logging; keep loop running
                    try:
                        print(f"SQLite insert error: {e}")
                    except Exception:
                        pass

            schedule.run_pending()

            # Light sleep to yield; workers block on DOM mutations when enabled
            if any(getattr(g, 'dom_wait', False) for g in self.grabbers.values()):
                time.sleep(0.05)
            else:
                time.sleep(0.5)

    def _checkpoint_wal(self):
        try:
            # This truncates the WAL file when possible
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass

    def _worker_loop(self, sport: str):
        g = self.grabbers[sport]
        while True:
            success, html = g.query()
            if success:
                self.queue.put((sport, datetime.now(), html))
            else:
                # In polling mode, avoid tight spin
                if not getattr(g, 'dom_wait', False):
                    time.sleep(0.25)
    
    def restart_grabbers(self):
        print('Restarting grabbers on daily schedule')
        for sport in self.sports:
            new_url = Recorder.build_url(sport)
            self.grabbers[sport].restart(url=new_url)

    @staticmethod
    def build_url(sport):
        gameday = date.today()  # Today's date
        url = (f'https://stats.ncaa.org/contests/livestream_scoreboards'
               f'?sport_code={SPORTS_CODE[sport]}&game_date={gameday.month}%2F{gameday.day}%2F{gameday.year}')
        return url

def recorder_main(config_path):
    # High level code with whole controller
    recorder = Recorder(config_path, WebGrabber)
    
    recorder.run()


if __name__ == '__main__':

    config_path = os.getenv("CONFIG_PATH", "config_debug.json")
    recorder_main(config_path=config_path)
