import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()
DB_NAME = os.getenv("DB_NAME")


def get_connection():
    """Returns a connection to the SQLite database specified in .env"""
    print(f"🔗 Connecting to DB: {DB_NAME}")
    return sqlite3.connect(DB_NAME)


def create_schema(conn):
    """Creates tables for Teams, Players, Seasons, Rosters, and Games"""
    cur = conn.cursor()
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Teams (
        id STRING NOT NULL PRIMARY KEY UNIQUE,
        name TEXT UNIQUE,
        short_name TEXT UNIQUE,
        abbreviation TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Players (
        player_id STRING NOT NULL PRIMARY KEY UNIQUE,
        player_name TEXT, 
        birth_date DATE,
        height_ft INTEGER,
        height_in INTEGER,
        nationality TEXT,
        primary_broad_position TEXT, 
        primary_general_position TEXT,
        secondary_broad_position TEXT, 
        secondary_general_position TEXT,
        season_name TEXT
    );

    CREATE TABLE IF NOT EXISTS Seasons (
        season_id INTEGER NOT NULL PRIMARY KEY UNIQUE,
        season_name STRING UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Rosters (
        roster_id INTEGER NOT NULL PRIMARY KEY UNIQUE,
        season_name STRING UNIQUE,
        team_name STRING,
        player_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS Games (
        game_id STRING NOT NULL PRIMARY KEY UNIQUE,
        date_time_utc DATE, 
        home_score INTEGER, 
        away_score INTEGER,
        home_team_id INTEGER,
        away_team_id INTEGER,
        referee_id INTEGER,
        stadium_id INTEGER,
        home_manager_id INTEGER,
        away_manager_id INTEGER,
        expanded_minutes INTEGER,
        season_name STRING,
        matchday DATE,
        attendance INTEGER,
        knockout_game STRING,
        status STRING,
        last_updated_utc DATE
    );

    CREATE TABLE IF NOT EXISTS Referees (
        referee_id STRING NOT NULL PRIMARY KEY UNIQUE,
        referee_name TEXT,
        nationality TEXT
    );

    CREATE TABLE IF NOT EXISTS Managers (
        manager_id STRING NOT NULL PRIMARY KEY UNIQUE,
        manager_name TEXT,
        nationality TEXT
    );


    CREATE TABLE IF NOT EXISTS Stadiums (
        stadium_id STRING NOT NULL PRIMARY KEY UNIQUE,
        stadium_name TEXT,
        year_built INTEGER,
        capacity INTEGER,
        roof BOOLEAN,
        turf BOOLEAN,
        street TEXT,
        city TEXT,
        province TEXT,
        country TEXT,
        postal_code TEXT,
        latitude REAL,
        longitude REAL,
        field_x INTEGER,
        field_y INTEGER
    );

    CREATE TABLE IF NOT EXISTS PlayerXGoalsGK (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id TEXT,
        team_id TEXT,  -- Can be a string or JSON-encoded array
        minutes_played INTEGER,
        shots_faced INTEGER,
        goals_conceded INTEGER,
        saves INTEGER,
        share_headed_shots REAL,
        xgoals_gk_faced REAL,
        goals_minus_xgoals_gk REAL,
        goals_divided_by_xgoals_gk REAL
    );

    CREATE TABLE IF NOT EXISTS PlayerGoalsAdded (
        player_id TEXT,
        team_id TEXT,
        general_position TEXT,
        minutes_played INTEGER,
        PRIMARY KEY (player_id)
    );

    CREATE TABLE IF NOT EXISTS PlayerGoalsAddedActions (
        player_id TEXT,
        action_type TEXT,
        goals_added_raw REAL,
        goals_added_above_avg REAL,
        count_actions INTEGER,
        PRIMARY KEY (player_id, action_type),
        FOREIGN KEY (player_id) REFERENCES PlayerGoalsAdded(player_id)
    );

    ''')
    conn.commit()
    print("✅ Schema created successfully, again")
