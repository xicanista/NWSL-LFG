# Pulls team/match data from ASA API

# https://github.com/American-Soccer-Analysis
# https://app.americansocceranalysis.com/api/v1/__docs__/
# https://app.americansocceranalysis.com/api/v1/openapi.json

# from itscalledsoccer.client import AmericanSoccerAnalysis

# asa_client = AmericanSoccerAnalysis()

# asa_players = asa_client.get_players(names="Abby")
# print(asa_players)

import requests
import sqlite3

# Setup the database
conn = sqlite3.connect('nwslfandomdb.sqlite')
cur = conn.cursor()

# Do some setup
cur.executescript('''
    DROP TABLE IF EXISTS Teams;
    DROP TABLE IF EXISTS Players;

    CREATE TABLE Teams (
        id     TEXT PRIMARY KEY,
        name   TEXT UNIQUE,
        short_name  TEXT UNIQUE,
        abbreviation  TEXT UNIQUE
    );

    CREATE TABLE Players (
        id  TEXT PRIMARY KEY,
        name    TEXT, 
        birthdate   DATE,
        nationality TEXT,
        primary_broad_position    TEXT, 
        primary_general_position    TEXT,
        secondary_broad_position    TEXT, 
        secondary_general_position    TEXT,
        seasons TEXT
    );      

    ''')

# Set the endpoint URL
url = "https://app.americansocceranalysis.com/api/v1/nwsl/teams"

# Make a GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()  # Parse the JSON response
    print("request successful")
    # print(data)  # You can also loop through it, or save it, etc.

    for entry in data:
        team_id = entry['team_id']
        team_name = entry['team_name']
        team_short_name = entry['team_short_name']
        team_abbreviation = entry['team_abbreviation']

        print((team_id, team_name, team_short_name, team_abbreviation))

        cur.execute('''INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
            VALUES ( ?, ?, ?, ? )''', (team_id, team_name, team_short_name, team_abbreviation))

    conn.commit()

else:
    print("Request failed with status code {response.status_code}")

# debug
x = 1

