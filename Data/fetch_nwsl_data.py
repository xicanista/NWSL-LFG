# Pulls team/match data from ASA API

# https://github.com/American-Soccer-Analysis
# https://app.americansocceranalysis.com/api/v1/__docs__/
# https://app.americansocceranalysis.com/api/v1/openapi.json

# from itscalledsoccer.client import AmericanSoccerAnalysis

# asa_client = AmericanSoccerAnalysis()

# asa_players = asa_client.get_players(names="Abby")
# print(asa_players)

import requests
import json
import sqlite3

# Setup the database
conn = sqlite3.connect('nwslfandomdb.sqlite')
cur = conn.cursor()

# Do some setup
cur.executescript('''
    DROP TABLE IF EXISTS Teams;
    DROP TABLE IF EXISTS Players;
    DROP TABLE IF EXISTS Seasons;

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
        player_seasons TEXT
    );      

    CREATE TABLE Seasons (
        season_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
        season_name STRING UNIQUE,
        players STRING,
        teams   STRING
    );
    
    ''')

# Set the endpoint URL
urls = ["https://app.americansocceranalysis.com/api/v1/nwsl/teams", "https://app.americansocceranalysis.com/api/v1/nwsl/players"]

urlcount = 0

# Make a GET request
for url in urls:
    print("the url is ", url)
    # Check if the request was successful
    response = requests.get(urls[urlcount])

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response

        if urls[urlcount] == "https://app.americansocceranalysis.com/api/v1/nwsl/teams":
            for entry in data:
                team_id = entry.get('team_id')
                team_name = entry.get('team_name')
                team_short_name = entry.get('team_short_name')
                team_abbreviation = entry.get('team_abbreviation')

                print((team_id, team_name, team_short_name, team_abbreviation))

                cur.execute('''INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
                    VALUES ( ?, ?, ?, ? )''', (team_id, team_name, team_short_name, team_abbreviation))

            conn.commit()

        if urls[urlcount] == "https://app.americansocceranalysis.com/api/v1/nwsl/players":

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
                player_seasons = entry.get('season_name')

                # Serialize the player_seasons list
                if player_seasons is None:
                    player_seasons_json = json.dumps([])
                else:
                    player_seasons_json = json.dumps(player_seasons)

                print("the json ", player_seasons_json)

                print((player_id, player_name, player_height_ft, player_height_in, player_bday, player_nationality,
                       player_primary_broad_position, player_primary_general_position, player_secondary_broad_position,
                       player_secondary_general_position))

                cur.execute('''INSERT OR IGNORE INTO Players (player_id, player_name, height_ft, 
                    height_in, birth_date, nationality, primary_broad_position, primary_general_position,
                    secondary_broad_position, secondary_general_position, player_seasons) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (player_id, player_name, player_height_ft, player_height_in, player_bday, player_nationality, player_primary_broad_position, player_primary_general_position, player_secondary_broad_position, player_secondary_general_position, player_seasons_json))

                #try:
                #    print("player_seasons", player_seasons)
                #except:
                #    print("failure")
                #    continue

                conn.commit()

        urlcount +=1




    # Add seasons to Teams

else:
    print("Request failed with status code {response.status_code}")

# Build the list
season = 2013
thisyear = 2026
count = 1

while thisyear > 2013:  # 2013 is the NWSL's inaugural season
    #    season.append(thisyear)
    print("season: ", type(season), season)
    print("thisyear: ", type(thisyear), thisyear)

    cur.execute('''INSERT OR IGNORE INTO Seasons (season_id, season_name, players, teams)
                        VALUES ( ?, ?, ?, ? )''', (count, str(thisyear), None, None))
    thisyear -= 1
    count += 1

conn.commit()
conn.close()