CREATE DATABASE sportsiot;

\c sportsiot;

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
    UID int,
    Followed_School varchar(100),
    Followed_Sport varchar(50),
    Primary Key(UID, Followed_School, Followed_Sport),
    FOREIGN KEY(Followed_School, Followed_Sport) REFERENCES School(Name, sport)
);

CREATE table Game(
    date Date,
    time timestamp,
    Away_Team varchar(100),
    Home_Team varchar(100),
    Score json,
    Winner varchar(100),
    Sport varchar(50),
    Primary Key(date, Home_Team, Away_Team, Sport),
    FOREIGN KEY(Away_Team, sport) REFERENCES School(Name, sport),
    FOREIGN KEY(Home_Team, sport) REFERENCES School(Name, sport),
    FOREIGN KEY(Winner, sport) REFERENCES School(Name, sport)
);

NOTIFY "notify_channel"; --Might need parmater for a return string

CREATE OR REPLACE FUNCTION notify_game_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Send notification
    PERFORM pg_notify('notify_channel', row_to_json(NEW)::TEXT);
    
    -- Return the full new row object
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER game_changes_trigger
AFTER INSERT OR UPDATE
ON Game
FOR EACH ROW
EXECUTE FUNCTION notify_game_changes();
