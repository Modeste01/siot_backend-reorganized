import psycopg2
import json

with open("sample_data.json", "r", encoding="utf-8") as file:
    data = json.load(file)

DB_NAME = "sportsiot"
DB_USER = "root"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "5432"

try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    for sport in data["Sport"]:
        cur.execute(
            "INSERT INTO Sport (Name) VALUES (%s) ON CONFLICT (Name) DO NOTHING;",
            (sport["Name"],)
        )

    for school in data["School"]:
        cur.execute(
            "INSERT INTO School (Name, Sport) VALUES (%s, %s) ON CONFLICT (Name, Sport) DO NOTHING;",
            (school["Name"], school["Sport"])
        )

    for user in data["DeviceUser"]:
        cur.execute(
            "INSERT INTO DeviceUser (UID, Followed_School, Followed_Sport) VALUES (%s, %s, %s) ON CONFLICT (UID, Followed_School, Followed_Sport) DO NOTHING;",
            (user["UID"], user["Followed_School"], user["Followed_Sport"])
        )

    for game in data["Game"]:
        cur.execute(
            "INSERT INTO Game (date, time, Away_Team, Home_Team, Score, Winner, Sport) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Home_Team, Away_Team, Sport) DO NOTHING;",
            (
                game["date"], 
                game["time"], 
                game["Away_Team"], 
                game["Home_Team"], 
                json.dumps(game["Score"]),
                game["Winner"], 
                game["Sport"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data successfully inserted into the database!")

except Exception as e:
    print(f"❌ Error: {e}")
