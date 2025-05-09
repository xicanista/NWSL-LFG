# core/fetch_nwsl_data.py

import requests
import json
import logging

# =======================
# Cleaned Data Fetch Module
# =======================

def init_seasons_data(conn, firstseason, thisyear):
    cur = conn.cursor()
    count = 1
    while thisyear >= firstseason:
        cur.execute('''INSERT OR IGNORE INTO Seasons (season_id, season_name)
                        VALUES (?, ?)''', (count, str(thisyear)))
        thisyear -= 1
        count += 1
    conn.commit()

def fetch_team_data(conn, team_url):
    try:
        response = requests.get(team_url)
        logging.info(f"API response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
    except Exception:
        logging.exception("Failed to fetch team data")
        return

    cur = conn.cursor()
    for entry in data:
        cur.execute('''INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
                       VALUES (?, ?, ?, ?)''',
                    (entry.get('team_id'), entry.get('team_name'), entry.get('team_short_name'), entry.get('team_abbreviation')))
    conn.commit()

def insert_players(conn, data):
    print("inside insert_players - data value", data)
    cur = conn.cursor()

    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Players (
                player_id, player_name, height_ft, height_in, birth_date,
                nationality, primary_broad_position, primary_general_position,
                secondary_broad_position, secondary_general_position, season_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get('player_id'),
            entry.get('player_name'),
            entry.get('height_ft'),
            entry.get('height_in'),
            entry.get('birth_date'),
            entry.get('nationality'),
            entry.get('primary_broad_position'),
            entry.get('primary_general_position'),
            entry.get('secondary_broad_position'),
            entry.get('secondary_general_position'),
            json.dumps(entry.get('season_name'))
        ))
        print("inside insert_players")
    conn.commit()

def insert_players(conn, data):
    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Players (
                player_id, player_name, height_ft, height_in, birth_date,
                nationality, primary_broad_position, primary_general_position,
                secondary_broad_position, secondary_general_position, season_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get('player_id'),
            entry.get('player_name'),
            entry.get('height_ft'),
            entry.get('height_in'),
            entry.get('birth_date'),
            entry.get('nationality'),
            entry.get('primary_broad_position'),
            entry.get('primary_general_position'),
            entry.get('secondary_broad_position'),
            entry.get('secondary_general_position'),
            json.dumps(entry.get('season_name'))
        ))
    conn.commit()

def fetch_games_data(conn, base_url, season_name):
    try:
        call = f"{base_url}?season_name={season_name}"
        response = requests.get(call)
        logging.info(f"API response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
    except Exception:
        logging.exception(f"Failed to fetch games data for season {season_name}")
        return

    cur = conn.cursor()
    for entry in data:
        cur.execute('''INSERT OR IGNORE INTO Games (game_id, date_time_utc, home_score, 
                        away_score, home_team_id, away_team_id, referee_id, stadium_id, 
                        home_manager_id, away_manager_id, expanded_minutes, season_name, 
                        matchday, attendance, knockout_game, status, last_updated_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        entry.get('game_id'),
                        entry.get('date_time_utc'),
                        entry.get('home_score'),
                        entry.get('away_score'),
                        entry.get('home_team_id'),
                        entry.get('away_team_id'),
                        entry.get('referee_id'),
                        entry.get('stadium_id'),
                        entry.get('home_manager_id'),
                        entry.get('away_manager_id'),
                        entry.get('expanded_minutes'),
                        entry.get('season_name'),
                        entry.get('matchday'),
                        entry.get('attendance'),
                        entry.get('knockout_game'),
                        entry.get('status'),
                        entry.get('last_updated_utc')
                    ))
    conn.commit()
