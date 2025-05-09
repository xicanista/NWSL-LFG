import requests
import logging
import json

def fetch_data(url):
    try:
        response = requests.get(url)
        logging.info(f"API GET {url} returned {response.status_code}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.exception(f"Failed to fetch data from {url}")
        return []  # Return empty list to avoid crashing pipeline


def insert_teams(conn, data):
    cur = conn.cursor()
    for entry in data:
        cur.execute('''INSERT OR IGNORE INTO Teams (id, name, short_name, abbreviation)
                       VALUES (?, ?, ?, ?)''',
                       (entry['team_id'], entry['team_name'], entry['team_short_name'], entry['team_abbreviation']))
    conn.commit()

import json

def insert_players(conn, data):
    print("📥 insert_players called")
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
            json.dumps(entry.get('season_name'))  # in case this is a list
        ))
    conn.commit()
    print("✅ Players inserted:", len(data))


def insert_games(conn, data):
    print("📥 insert_games called")
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
    print("✅ Games inserted:", len(data))


def insert_managers(conn, data):
    print("📥 insert_managers called")
    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Managers (
                manager_id, manager_name, nationality
            ) VALUES (?, ?, ?)
        ''', (
            entry.get('manager_id'),
            entry.get('manager_name'),
            entry.get('nationality')
        ))
    conn.commit()
    print("✅ Managers inserted:", len(data))

def insert_referees(conn, data):
    print("📥 insert_referees called")
    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Referees (
                referee_id, referee_name, nationality
            ) VALUES (?, ?, ?)
        ''', (
            entry.get('referee_id'),
            entry.get('referee_name'),
            entry.get('nationality')
        ))
    conn.commit()
    print("✅ Referees inserted:", len(data))

def insert_stadiums(conn, data):
    print("📥 insert_stadiums called")
    cur = conn.cursor()
    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Stadiums (
                stadium_id, stadium_name, year_built, capacity, roof, turf,
                street, city, province, country, postal_code,
                latitude, longitude, field_x, field_y
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get('stadium_id'),
            entry.get('stadium_name'),
            entry.get('year_built'),
            entry.get('capacity'),
            entry.get('roof'),
            entry.get('turf'),
            entry.get('street'),
            entry.get('city'),
            entry.get('province'),
            entry.get('country'),
            entry.get('postal_code'),
            entry.get('latitude'),
            entry.get('longitude'),
            entry.get('field_x'),
            entry.get('field_y')
        ))
    conn.commit()
    print("✅ Stadiums inserted:", len(data))

def insert_player_xgoals_gk(conn, data):
    print("📥 insert_player_xgoals_gk called")
    cur = conn.cursor()
    for entry in data:
        team_id = entry.get("team_id")
        if isinstance(team_id, list):
            team_id = json.dumps(team_id)

        cur.execute('''
            INSERT INTO PlayerXGoalsGK (
                player_id, team_id, minutes_played, shots_faced,
                goals_conceded, saves, share_headed_shots,
                xgoals_gk_faced, goals_minus_xgoals_gk, goals_divided_by_xgoals_gk
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                entry.get("player_id"),
                team_id,
                entry.get("minutes_played"),
                entry.get("shots_faced"),
                entry.get("goals_conceded"),
                entry.get("saves"),
                entry.get("share_headed_shots"),
                entry.get("xgoals_gk_faced"),
                entry.get("goals_minus_xgoals_gk"),
                entry.get("goals_divided_by_xgoals_gk")
        ))
    conn.commit()
    print(f"✅ Player GK xG entries inserted: {len(data)}")

def insert_player_goals_added(conn, data):
    print("📥 insert_player_goals_added called")
    cur = conn.cursor()

    for player in data:
        # Normalize team_id to string
        team_id = player.get("team_id")
        if isinstance(team_id, list):
            team_id = json.dumps(team_id)

        # Insert summary row
        cur.execute('''
            INSERT OR IGNORE INTO PlayerGoalsAdded (
                player_id, team_id, general_position, minutes_played
            ) VALUES (?, ?, ?, ?)
        ''', (
            player.get("player_id"),
            team_id,
            player.get("general_position"),
            player.get("minutes_played")
        ))

        # Insert each action type
        for action in player.get("data", []):
            cur.execute('''
                INSERT OR IGNORE INTO PlayerGoalsAddedActions (
                    player_id, action_type, goals_added_raw,
                    goals_added_above_avg, count_actions
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                player.get("player_id"),
                action.get("action_type"),
                action.get("goals_added_raw"),
                action.get("goals_added_above_avg"),
                action.get("count_actions")
            ))

    conn.commit()
    print(f"✅ PlayerGoalsAdded inserted: {len(data)}")

    conn.commit()
    print(f"✅ PlayerGoalsAdded inserted: {len(data)}")

print("save me")


