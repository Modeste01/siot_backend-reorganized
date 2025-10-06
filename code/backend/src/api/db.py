import psycopg2
from datetime import date, datetime, time
import json  # JSON parsing

class Database:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        self.cur = self.conn.cursor()

    def is_device_approved(self,device_id):
        self.cur.execute("")

    def _json_serial(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.strftime("%H:%M:%S")
        if isinstance(obj, (list, dict)):
            return json.dumps(obj)
        if obj is None:
            return None
        raise TypeError(f"Type {type(obj)} not serializable: {obj}")

    def _parse_rows(self, rows):
        col_names = [desc[0] for desc in self.cur.description]
        result = [dict(zip(col_names, row)) for row in rows]
        json_output = json.dumps(result, indent=4, default=self._json_serial)
        return json.loads(json_output)
    
    def get_games(self):
        self.cur.execute("SELECT date, time, Away_Team, Home_Team, Score, Winner, Sport FROM Game")
        rows = self.cur.fetchall()

        parsed_rows = []
        for row in rows:
            row = list(row)

            if row[4]:
                if isinstance(row[4], str):
                    try:
                        row[4] = json.loads(row[4])
                    except (json.JSONDecodeError, TypeError):
                        row[4] = {}

            parsed_rows.append(tuple(row))

        return self._parse_rows(parsed_rows)

    def get_games_with_team(self, team_name):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE away_team = %s OR home_team = %s;
        """

        self.cur.execute(query, (team_name, team_name))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)

    def get_games_by_sport(self, sport_name):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE sport = %s;
        """
        self.cur.execute(query, (sport_name,))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)

    def get_games_by_date(self, game_date):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE date = %s;
        """
        self.cur.execute(query, (game_date,))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)

    def get_games_by_time(self, game_time):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE time = %s;
        """
        self.cur.execute(query, (game_time,))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)

    def get_games_by_score(self, min_score):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE CAST(score->>'home' AS INTEGER) >= %s OR CAST(score->>'away' AS INTEGER) >= %s;
        """
        self.cur.execute(query, (min_score, min_score))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)
    
    def get_games_by_date_and_time(self, game_date, game_time):
        query = """
            SELECT date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE date = %s AND time = %s;
        """
        self.cur.execute(query, (game_date, game_time))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)
    
    def get_teams_playing_on_date(self, game_date):
        query = """
            SELECT DISTINCT away_team, home_team
            FROM Game
            WHERE date = %s;
        """
        self.cur.execute(query, (game_date,))
        teams = self.cur.fetchall()
        team_list = set()
        for away, home in teams:
            team_list.add(away)
            team_list.add(home)
        return list(team_list)

    def get_sports_playing_on_date(self, game_date):
        query = """
            SELECT DISTINCT sport
            FROM Game
            WHERE date = %s;
        """
        self.cur.execute(query, (game_date,))
        sports = self.cur.fetchall()
        return [sport[0] for sport in sports]

    def get_followed_games(self, device_uid):
        query = """
            SELECT g.date, g.time, g.away_team, g.home_team, g.score, g.winner, g.sport
            FROM Game g
            JOIN deviceuser du ON (g.sport = du.followed_sport AND (g.home_team = du.followed_school OR g.away_team = du.followed_school))
            WHERE du.uid = %s;
        """
        self.cur.execute(query, (device_uid,))
        games = self.cur.fetchall()
        
        parsed_games = []
        for game in games:
            game = list(game)
            if game[4]:  # Score field
                if isinstance(game[4], str):
                    try:
                        game[4] = json.loads(game[4])
                    except (json.JSONDecodeError, TypeError):
                        game[4] = {}
            parsed_games.append(tuple(game))
        
        return self._parse_rows(parsed_games)

    def get_game_by_id(self, game_id: int):
        query = """
            SELECT id, date, time, away_team, home_team, score, winner, sport
            FROM Game WHERE id = %s
        """
        self.cur.execute(query, (game_id,))
        row = self.cur.fetchone()
        if not row:
            return None
        # Normalize score JSON if necessary
        row = list(row)
        if row[5] and isinstance(row[5], str):
            try:
                row[5] = json.loads(row[5])
            except Exception:
                pass
        col_names = [desc[0] for desc in self.cur.description]
        return dict(zip(col_names, row))

    def get_latest_games_for_team_by_sports(self, school: str, sports: list):
        results = []
        for sport in sports or []:
            self.cur.execute(
                """
                SELECT id, date, time, away_team, home_team, score, winner, sport
                FROM Game
                WHERE sport = %s AND (home_team = %s OR away_team = %s)
                ORDER BY date DESC, time DESC NULLS LAST
                LIMIT 1
                """,
                (sport, school, school)
            )
            row = self.cur.fetchone()
            if not row:
                continue
            row = list(row)
            if row[5] and isinstance(row[5], str):
                try:
                    row[5] = json.loads(row[5])
                except Exception:
                    pass
            col_names = [desc[0] for desc in self.cur.description]
            results.append(dict(zip(col_names, row)))
        return results

    def get_recent_games_for_team_by_sports(self, school: str, sports: list, hours: int = 24):
        if not sports:
            return []
        # Use ANY with array parameter for sports list
        interval_str = f"{hours} hours"
        self.cur.execute(
            """
            SELECT id, date, time, away_team, home_team, score, winner, sport
            FROM Game
            WHERE sport = ANY(%s)
              AND (home_team = %s OR away_team = %s)
              AND COALESCE(time, date::timestamp) >= (NOW() - INTERVAL %s)
            ORDER BY COALESCE(time, date::timestamp) DESC
            """,
            (sports, school, school, interval_str)
        )
        rows = self.cur.fetchall() or []
        results = []
        for row in rows:
            row = list(row)
            if row[5] and isinstance(row[5], str):
                try:
                    row[5] = json.loads(row[5])
                except Exception:
                    pass
            col_names = [desc[0] for desc in self.cur.description]
            results.append(dict(zip(col_names, row)))
        return results


    def add_game(self, game):
        insert_query = """
            INSERT INTO Game (date, time, away_team, home_team, score, winner, sport)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        select_query = """
            SELECT * FROM Game WHERE away_team = %s AND home_team = %s AND sport = %s AND date = %s 
        """

        update_query = """
            UPDATE Game
            SET score = %s, winner = %s, time = %s
            WHERE away_team = %s AND home_team = %s AND sport = %s AND date = %s
        """

        self.cur.execute(select_query, (
            game["away_team"],
            game["home_team"],
            game["sport"],
            game["date"]
        )) 

        exists = len(self.cur.fetchall()) != 0

        score_json = json.dumps(game["score"]) if isinstance(game["score"], dict) else game["score"]
        if not exists:
            self.cur.execute(insert_query, (
                game["date"],
                game["time"],
                game["away_team"],
                game["home_team"],
                score_json,
                game["winner"],
                game["sport"]
            ))
        else:
            self.cur.execute(update_query, (
                score_json,
                game["winner"],
                game["time"],
                game["away_team"],
                game["home_team"],
                game["sport"],
                game["date"]
            ))

        self.conn.commit()

    def update_game_winner(self, game_id, winner):
        query = """
            UPDATE Game
            SET winner = %s
            WHERE id = %s
        """
        self.cur.execute(query, (winner, game_id))
        self.conn.commit()
        return self.cur.rowcount

    def get_id_by_team(self, team, sport):
        self.cur.execute("SELECT uid FROM deviceuser WHERE followed_school = %s and followed_sport = %s", (team, sport))
        targetUsers = self.cur.fetchall()

        #Convert the list of tuples returned by fetchall into a string of ids separated by commas
        res = ""
        for user in targetUsers:
            res += str(user[0])
            res += ","

        res = res[:-1] #removes the last comma
        return res

    def close(self):
        self.conn.close()


    def insert_school(self, name, sport):
        try:
            self.cur.execute("SELECT name, sport FROM school WHERE name = %s AND sport = %s", (name, sport))
        except Exception:
            self.conn.rollback()
            self.cur.execute("SELECT name, sport FROM school WHERE name = %s AND sport = %s", (name, sport))

        exists = False
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == name and row[1] == sport:
                exists = True

        if not exists:
            try:
                self.cur.execute("INSERT INTO school (name, sport) VALUES (%s, %s)", (name, sport))
                self.conn.commit()
            except Exception:
                self.conn.rollback()
        else:
            # No-op but ensure clean transaction
            self.conn.commit()

    def insert_game(self, g):
        self.cur.execute(
            """
            SELECT home_team, away_team, sport FROM game WHERE
            home_team = %s AND away_team = %s AND sport = %s
            """,
            (g["home_team"],
             g["away_team"],
             g["sport"])
        )

        exists = False
        rows = self.cur.fetchall()
        for row in rows:
            exists = True
            break

        if exists:
            self.cur.execute(
                """
                UPDATE game
                SET date = %s, time = %s, score = %s, winner = %s
                WHERE home_team = %s AND away_team = %s AND sport = %s
                """,
                (g["date"],
                g["time"],
                json.dumps(g["score"]),
                g["winner"],
                g["home_team"],
                g["away_team"],
                g["sport"])
            )
            
        else:
            self.cur.execute(
                """
                INSERT INTO game
                (date, time, away_team, home_team, score, winner, sport)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (g["date"],
                g["time"],
                g["away_team"],
                g["home_team"],
                json.dumps(g["score"]),
                g["winner"],
                g["sport"])
            )

        self.conn.commit()

    def delete_game_by_id(self, game_id: int):
        self.cur.execute("DELETE FROM Game WHERE id = %s", (game_id,))
        deleted = self.cur.rowcount > 0
        self.conn.commit()
        return deleted

    def insert_sport(self, name):
        self.cur.execute("SELECT name FROM sport WHERE name = %s", (name,))

        exists = False
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == name:
                exists = True

        if not exists:
            self.cur.execute("INSERT INTO sport (name) VALUES (%s)", (name,))

        self.conn.commit()

    def get_user(self, uid):
        query = """
            SELECT followed_school, followed_sport 
            FROM deviceuser
            WHERE uid = %s
        """

        self.cur.execute(query, (uid,))

        rows = self.cur.fetchall()

        return rows

    def set_follow(self, uid: str, school: str, sport: str):
        try:
            self.cur.execute(
                """
                INSERT INTO deviceuser (uid, followed_school, followed_sport)
                VALUES (%s, %s, %s)
                ON CONFLICT (uid, followed_school, followed_sport) DO NOTHING
                """,
                (uid, school, sport)
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def delete_follow(self, uid: str, school: str, sport: str):
        try:
            self.cur.execute(
                "DELETE FROM deviceuser WHERE uid = %s AND followed_school = %s AND followed_sport = %s",
                (uid, school, sport)
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # --- Device registration and connection state ---
    def upsert_device(self, uid: str, school: str | None):
        try:
            self.cur.execute(
                """
                INSERT INTO device (uid, school, connected, last_connect, last_seen)
                VALUES (%s, %s, false, NULL, NOW())
                ON CONFLICT (uid) DO UPDATE SET school = EXCLUDED.school, last_seen = NOW()
                """,
                (uid, school)
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def mark_connected(self, uid: str):
        try:
            self.cur.execute(
                "UPDATE device SET connected = true, last_connect = NOW(), last_seen = NOW() WHERE uid = %s",
                (uid,)
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def mark_disconnected(self, uid: str):
        try:
            self.cur.execute(
                "UPDATE device SET connected = false, last_disconnect = NOW(), last_seen = NOW() WHERE uid = %s",
                (uid,)
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def replace_follows(self, uid: str, school: str, sports: list[str]):
        try:
            # Ensure School rows exist
            for sp in sports:
                self.insert_school(school, sp)
            # Remove any follows not in the new list
            self.cur.execute(
                "DELETE FROM deviceuser WHERE uid = %s AND followed_school = %s AND followed_sport <> ALL(%s)",
                (uid, school, sports if sports else ['__none__'])
            )
            # Add (or keep) new follows
            for sp in sports:
                self.set_follow(uid, school, sp)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
