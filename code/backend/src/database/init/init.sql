-- Using the default database provided by POSTGRES_DB (sportsiot) from the container env.

CREATE table School(
    Name varchar(100),
    Sport varchar(50),
    Primary Key(Name, Sport)
);

CREATE table Sport(
    Name varchar(50),
    Primary Key(Name)
);

CREATE table DeviceUser(
  UID varchar(64),
  Followed_School varchar(100),
  Followed_Sport varchar(50),
  Primary Key(UID, Followed_School, Followed_Sport),
  FOREIGN KEY(Followed_School, Followed_Sport) REFERENCES School(Name, sport)
);

CREATE table Game(
  id BIGSERIAL PRIMARY KEY,
  date Date,
  time timestamp,
  Away_Team varchar(100),
  Home_Team varchar(100),
  Score jsonb,
  Winner varchar(100),
  Sport varchar(50),
  UNIQUE (Home_Team, Away_Team, Sport),
  FOREIGN KEY(Away_Team, sport) REFERENCES School(Name, sport),
  FOREIGN KEY(Home_Team, sport) REFERENCES School(Name, sport),
  FOREIGN KEY(Winner, sport) REFERENCES School(Name, sport)
);

NOTIFY "notify_channel"; --Might need parmater for a return string

CREATE OR REPLACE FUNCTION notify_gameinserted()
  RETURNS trigger AS $$
DECLARE
BEGIN
  PERFORM pg_notify(
    'notify_channel',
    row_to_json(NEW)::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_gameinserted
  AFTER INSERT ON Game
  FOR EACH ROW
  EXECUTE PROCEDURE notify_gameinserted();

CREATE OR REPLACE FUNCTION notify_update() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('notify_channel', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_notify_trigger
AFTER UPDATE ON Game
FOR EACH ROW
EXECUTE FUNCTION notify_update();

-- Track device registrations and connection state
CREATE TABLE IF NOT EXISTS Device (
  uid varchar(64) PRIMARY KEY,
  school varchar(100),
  connected boolean DEFAULT false,
  last_connect timestamptz,
  last_disconnect timestamptz,
  last_seen timestamptz
);

