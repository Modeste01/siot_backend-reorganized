import psycopg2
import json

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

    def insert_school(self, name, sport):
        self.cur.execute("SELECT name, sport FROM school WHERE name = %s AND sport = %s", (name, sport))

        exists = False
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == name and row[1] == sport:
                exists = True

        if not exists:
            self.cur.execute("INSERT INTO school (name, sport) VALUES (%s, %s)", (name, sport))

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
