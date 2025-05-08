# Pulls team/match data from ASA API
from unittest import expectedFailure

# https://github.com/American-Soccer-Analysis
# https://app.americansocceranalysis.com/api/v1/__docs__/
# https://app.americansocceranalysis.com/api/v1/openapi.json

# from itscalledsoccer.client import AmericanSoccerAnalysis

# asa_client = AmericanSoccerAnalysis()

# asa_players = asa_client.get_players(names="Abby")
# print(asa_players)

from dotenv import load_dotenv
import os

load_dotenv()

ASA_API_BASE = os.getenv("ASA_API_BASE")
DB_NAME = os.getenv("DB_NAME")
LOG_FILE = os.getenv("LOG_FILE")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # fallback to INFO

import requests
import json
import sqlite3
import logging

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO)
)

# Setup the DB
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.executescript('''
    DROP TABLE IF EXISTS Teams;
    DROP TABLE IF EXISTS Players;
    DROP TABLE IF EXISTS Seasons;
    DROP TABLE IF EXISTS Rosters;
    DROP TABLE IF EXISTS Games;

    CREATE TABLE Teams (
        id     STRING NOT NULL PRIMARY KEY UNIQUE,
        name   TEXT UNIQUE,
        short_name  TEXT UNIQUE,
        abbreviation  TEXT UNIQUE
    );

    CREATE TABLE Players (
        player_id  STRING NOT NULL PRIMARY KEY UNIQUE,
        player_name TEXT, 
        birth_date   DATE,
        height_ft   INTEGER,
        height_in   INTEGER,
        nationality TEXT,
        primary_broad_position    TEXT, 
        primary_general_position    TEXT,
        secondary_broad_position    TEXT, 
        secondary_general_position    TEXT,
        season_name TEXT
    );      

    CREATE TABLE Seasons (
        season_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
        season_name STRING UNIQUE
    );
    
    CREATE TABLE Rosters (
        roster_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
        season_name STRING UNIQUE,
        team_name     STRING,
        player_id   INTEGER
    );
    
    CREATE TABLE Games (
        game_id STRING NOT NULL PRIMARY KEY UNIQUE,
        date_time_utc   DATE, 
        home_score  INTEGER, 
        away_score  INTEGER,
        home_team_id    INTEGER,
        away_team_id    INTEGER,
        referee_id  INTEGER,
        stadium_id  INTEGER,
        home_manager_id INTEGER,
        away_manager_id INTEGER,
        expanded_minutes    INTEGER,
        season_name STRING,
        matchday    DATE,
        attendance  INTEGER,
        knockout_game   STRING,
        status  STRING,
        last_updated_utc    DATE
    );
    
    ''')

# Pull data from API

def init_seasons_data(firstseason, thisyear):
    # Build the list
    #fs = firstseason
    #ty = thisyear

    count = 1

    while thisyear >= firstseason:  # 2013 is the NWSL's inaugural season

        cur.execute('''INSERT OR IGNORE INTO Seasons (season_id, season_name)
            VALUES ( ?, ?)''', (count, str(thisyear)))

        thisyear -= 1
        count += 1

    conn.commit()

def fetch_team_data(team):
    try:
        # Check if the request was successful
        response = requests.get(team)
        logging.info("API response status: {response.status_code}")
        logging.debug("API response body: {response.text}")

    except Exception:
        logging.exception("Failed to fetch data")
        logging.info("API response status: ", response.status_code)
        logging.debug("Request failed with status code : ", response.text)

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response

        for entry in data:
            team_id = entry.get('team_id')
            team_name = entry.get('team_name')
            team_short_name = entry.get('team_short_name')
            team_abbreviation = entry.get('team_abbreviation')

            cur.execute('''INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
                VALUES ( ?, ?, ?, ? )''', (team_id, team_name, team_short_name, team_abbreviation))

        conn.commit()

def fetch_player_data(players):
    try:
        # Check if the request was successful
        response = requests.get(players)
        logging.info("API response status: {response.status_code}")
        logging.debug("API response body: {response.text}")

    except Exception:
        logging.exception("Failed to fetch data")
        logging.info("API response status: ", response.status_code)
        logging.debug("Request failed with status code : ", response.text)

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response

        for entry in data:
            player_id = entry.get('player_id')
            player_name = entry.get('player_name')
            player_height_ft = entry.get('height_ft')
            player_height_in = entry.get('height_in')
            player_bday = entry.get('birth_date')
            player_nationality = entry.get('nationality')
            player_primary_broad_position = entry.get('primary_broad_position')
            player_primary_general_position = entry.get('primary_general_position')
            player_secondary_broad_position = entry.get('secondary_broad_position')
            player_secondary_general_position = entry.get('secondary_general_position')
            player_seasons_played = entry.get('season_name')

            cur.execute('''INSERT OR IGNORE INTO Players (player_id, player_name, height_ft, 
                height_in, birth_date, nationality, primary_broad_position, primary_general_position,
                secondary_broad_position, secondary_general_position, season_name) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (player_id, player_name, player_height_ft, player_height_in, player_bday, player_nationality, player_primary_broad_position, player_primary_general_position, player_secondary_broad_position, player_secondary_general_position, json.dumps(player_seasons_played)))

        conn.commit()

#def build_rosters(season, team_name):

    #cur.execute('''
    #    SELECT Players.season_name
    #    FROM Players
    #    JOIN Seasons ON Players.player_id = Rosters.player_id
    #    WHERE Seasons.season_name = ? AND Players.team_name = ?
    #    ''', (season, team_name))

    #rows = cur.fetchall()

    #print("rows ", rows)

def fetch_games_data(games, season_name):
    try:
        # Check if the request was successful
        call = games + "?season_name=" + season_name
        response = requests.get(call) # i.e. https://app.americansocceranalysis.com/api/v1/nwsl/games?season_name=2024
        logging.info("API response status: {response.status_code}")
        logging.debug("API response body: {response.text}")

    except Exception:
        logging.exception("Failed to fetch data")
        logging.info("API response status: ", response.status_code)
        logging.debug("Request failed with status code : ", response.text)

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response

    for entry in data:
        game_id = entry.get('game_id')
        game_date_time_utc = entry.get('date_time_utc')
        game_home_score = entry.get('home_score')
        game_away_score = entry.get('away_score')
        game_home_team_id = entry.get('home_team_id')
        game_away_team_id = entry.get('away_team_id')
        game_referee_id = entry.get('referee_id')
        game_stadium_id = entry.get('stadium_id')
        game_home_manager_id = entry.get('home_manager_id')
        game_away_manager_id = entry.get('away_manager_id')
        game_expanded_minutes = entry.get('expanded_minutes')
        game_season_name = entry.get('season_name')
        game_matchday = entry.get('matchday')
        game_attendance = entry.get('attendance')
        game_knockout_game = entry.get('knockout_game')
        game_status = entry.get('status')
        game_last_updated_utc = entry.get('last_updated_utc')


        cur.execute('''
            INSERT OR IGNORE INTO Games (game_id, date_time_utc, home_score, 
            away_score, home_team_id, away_team_id, referee_id, stadium_id, 
            home_manager_id, away_manager_id, expanded_minutes, season_name, 
            matchday, attendance, knockout_game, status, last_updated_utc)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (game_id, game_date_time_utc, game_home_score, game_away_score, game_home_team_id, game_away_team_id, game_referee_id, game_stadium_id, game_home_manager_id, game_away_manager_id, game_expanded_minutes, game_season_name, game_matchday, game_attendance, game_knockout_game, game_status, game_last_updated_utc))

    conn.commit()

# Fetch data and store in db
init_seasons_data(2013, 2025)
fetch_team_data(f"{ASA_API_BASE}/teams")
fetch_player_data(f"{ASA_API_BASE}/players")

for year in range(2015, 2026):  # 2026 is exclusive, so stops at 2025
    fetch_games_data(f"{ASA_API_BASE}/games", str(year))
