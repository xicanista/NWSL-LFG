# core/fetch_nwsl_data.py

import logging
import json
from core.asa_client import fetch_data

# =======================
# Data Fetch and Insert Utilities
# =======================

def fetch_team_data(conn, team_url):
    """
    Fetches team data from the given URL and inserts it into the Teams table.
    """
    try:
        data = fetch_data(team_url)
    except Exception:
        logging.exception("Failed to fetch team data")
        return

    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
            VALUES (?, ?, ?, ?)
        ''', (
            entry.get('team_id'),
            entry.get('team_name'),
            entry.get('team_short_name'),
            entry.get('team_abbreviation')
        ))
    conn.commit()


def fetch_games_data(conn, base_url, season_name):
    """
    Fetches games data for the specified season and inserts it into the Games table.
    """
    call = f"{base_url}?season_name={season_name}"
    try:
        data = fetch_data(call)
    except Exception:
        logging.exception(f"Failed to fetch games data for season {season_name}")
        return

    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Games (
                game_id, date_time_utc, home_score, away_score,
                home_team_id, away_team_id, referee_id, stadium_id,
                home_manager_id, away_manager_id, expanded_minutes,
                season_name, matchday, attendance, knockout_game,
                status, last_updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
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
