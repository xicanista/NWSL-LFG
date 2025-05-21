import requests
import logging

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



def insert_player_goals_added(conn, data):
    """Insert summary + per-action goals added data for each player without overwriting existing rows."""
    print("📥 insert_player_goals_added called")
    cur = conn.cursor()
    inserted_players = 0
    inserted_actions = 0

    for player in data:
        player_id = player.get("player_id")
        team_id = player.get("team_id")
        general_position = player.get("general_position")
        minutes_played = player.get("minutes_played")

        # Normalize team_id to string if it's a list
        if isinstance(team_id, list):
            team_id = json.dumps(team_id)

        # Insert into PlayerGoalsAdded (ignore if already exists)
        cur.execute('''
            INSERT OR IGNORE INTO PlayerGoalsAdded (
                player_id, team_id, general_position, minutes_played
            ) VALUES (?, ?, ?, ?)
        ''', (
            player_id, team_id, general_position, minutes_played
        ))
        inserted_players += cur.rowcount  # Only count if inserted

        # Insert each action type (ignore duplicates)
        for action in player.get("data", []):
            cur.execute('''
                INSERT OR IGNORE INTO PlayerGoalsAddedActions (
                    player_id, action_type, goals_added_raw,
                    goals_added_above_avg, count_actions
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                player_id,
                action.get("action_type"),
                action.get("goals_added_raw"),
                action.get("goals_added_above_avg"),
                action.get("count_actions")
            ))
            inserted_actions += cur.rowcount

    conn.commit()
    print(f"✅ Inserted {inserted_players} new player summary rows")
    print(f"✅ Inserted {inserted_actions} new player action rows")

def upsert_player_goals_added(conn, data):
    """Upsert PlayerGoalsAdded and PlayerGoalsAddedActions with change detection."""
    print("📥 upsert_player_goals_added called")
    cur = conn.cursor()
    updated_players = 0
    inserted_players = 0
    updated_actions = 0
    inserted_actions = 0

    for player in data:
        player_id = player.get("player_id")
        team_id = player.get("team_id")
        general_position = player.get("general_position")
        minutes_played = player.get("minutes_played")

        if isinstance(team_id, list):
            team_id = json.dumps(team_id)

        # Check existing PlayerGoalsAdded
        cur.execute('SELECT team_id, general_position, minutes_played FROM PlayerGoalsAdded WHERE player_id = ?', (player_id,))
        existing = cur.fetchone()

        if existing:
            if (existing[0] != team_id) or (existing[1] != general_position) or (existing[2] != minutes_played):
                cur.execute('''
                    UPDATE PlayerGoalsAdded
                    SET team_id = ?, general_position = ?, minutes_played = ?
                    WHERE player_id = ?
                ''', (team_id, general_position, minutes_played, player_id))
                updated_players += 1
        else:
            cur.execute('''
                INSERT INTO PlayerGoalsAdded (
                    player_id, team_id, general_position, minutes_played
                ) VALUES (?, ?, ?, ?)
            ''', (player_id, team_id, general_position, minutes_played))
            inserted_players += 1

        # Upsert PlayerGoalsAddedActions
        for action in player.get("data", []):
            action_type = action.get("action_type")
            goals_added_raw = action.get("goals_added_raw")
            goals_added_above_avg = action.get("goals_added_above_avg")
            count_actions = action.get("count_actions")

            cur.execute('''
                SELECT goals_added_raw, goals_added_above_avg, count_actions
                FROM PlayerGoalsAddedActions
                WHERE player_id = ? AND action_type = ?
            ''', (player_id, action_type))
            existing_action = cur.fetchone()

            if existing_action:
                if (
                    existing_action[0] != goals_added_raw or
                    existing_action[1] != goals_added_above_avg or
                    existing_action[2] != count_actions
                ):
                    cur.execute('''
                        UPDATE PlayerGoalsAddedActions
                        SET goals_added_raw = ?, goals_added_above_avg = ?, count_actions = ?
                        WHERE player_id = ? AND action_type = ?
                    ''', (
                        goals_added_raw, goals_added_above_avg, count_actions,
                        player_id, action_type
                    ))
                    updated_actions += 1
            else:
                cur.execute('''
                    INSERT INTO PlayerGoalsAddedActions (
                        player_id, action_type, goals_added_raw,
                        goals_added_above_avg, count_actions
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    player_id, action_type,
                    goals_added_raw, goals_added_above_avg, count_actions
                ))
                inserted_actions += 1

    conn.commit()
    print(f"✅ {inserted_players} players inserted, {updated_players} players updated")
    print(f"✅ {inserted_actions} actions inserted, {updated_actions} actions updated")

def insert_player_xpass(conn, data):
    print("📥 insert_player_xpass called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        team_id = entry.get("team_id")
        if isinstance(team_id, list):
            team_id = json.dumps(team_id)  # flatten to JSON string

        cur.execute('''
            INSERT OR IGNORE INTO PlayerXpass (
                player_id, team_id, general_position, minutes_played,
                attempted_passes, pass_completion_percentage,
                xpass_completion_percentage, passes_completed_over_expected,
                passes_completed_over_expected_p100, avg_distance_yds,
                avg_vertical_distance_yds, share_team_touches, count_games
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get("player_id"),
            team_id,
            entry.get("general_position"),
            entry.get("minutes_played"),
            entry.get("attempted_passes"),
            entry.get("pass_completion_percentage"),
            entry.get("xpass_completion_percentage"),
            entry.get("passes_completed_over_expected"),
            entry.get("passes_completed_over_expected_p100"),
            entry.get("avg_distance_yds"),
            entry.get("avg_vertical_distance_yds"),
            entry.get("share_team_touches"),
            entry.get("count_games")
        ))
        inserted += cur.rowcount

    conn.commit()
    print(f"✅ PlayerXpass entries inserted: {inserted}")

def insert_player_xgoals(conn, data):
    print("📥 insert_player_xgoals called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        team_id = entry.get("team_id")
        if isinstance(team_id, list):
            team_id = json.dumps(team_id)

        cur.execute('''
            INSERT OR IGNORE INTO PlayerXgoals (
                player_id, team_id, general_position, minutes_played,
                shots, shots_on_target, goals, xgoals, xplace,
                goals_minus_xgoals, key_passes, primary_assists,
                xassists, primary_assists_minus_xassists,
                goals_plus_primary_assists, xgoals_plus_xassists,
                points_added, xpoints_added
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get("player_id"),
            team_id,
            entry.get("general_position"),
            entry.get("minutes_played"),
            entry.get("shots"),
            entry.get("shots_on_target"),
            entry.get("goals"),
            entry.get("xgoals"),
            entry.get("xplace"),
            entry.get("goals_minus_xgoals"),
            entry.get("key_passes"),
            entry.get("primary_assists"),
            entry.get("xassists"),
            entry.get("primary_assists_minus_xassists"),
            entry.get("goals_plus_primary_assists"),
            entry.get("xgoals_plus_xassists"),
            entry.get("points_added"),
            entry.get("xpoints_added")
        ))
        inserted += cur.rowcount

    conn.commit()
    print(f"✅ PlayerXgoals entries inserted: {inserted}")

def insert_team_xgoals(conn, data):
    print("📥 insert_team_xgoals called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO TeamXGoals (
                team_id, count_games, shots_for, shots_against,
                goals_for, goals_against, goal_difference,
                xgoals_for, xgoals_against, xgoal_difference,
                goal_difference_minus_xgoal_difference, points, xpoints
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get("team_id"),
            entry.get("count_games"),
            entry.get("shots_for"),
            entry.get("shots_against"),
            entry.get("goals_for"),
            entry.get("goals_against"),
            entry.get("goal_difference"),
            entry.get("xgoals_for"),
            entry.get("xgoals_against"),
            entry.get("xgoal_difference"),
            entry.get("goal_difference_minus_xgoal_difference"),
            entry.get("points"),
            entry.get("xpoints")
        ))
        inserted += cur.rowcount

    conn.commit()
    print(f"✅ Team xGoals entries inserted: {inserted}")

def insert_player_xgoals_gk(conn, data):
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

def insert_game_xgoals(conn, data):
    print("📥 insert_game_xgoals called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO GameXgoals (
                game_id, home_team_id, away_team_id,
                xgoals_home, xgoals_away,
                home_goals, away_goals,
                xpoints_home, xpoints_away
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry.get("game_id"),
            entry.get("home_team_id"),
            entry.get("away_team_id"),
            entry.get("xgoals_home"),
            entry.get("xgoals_away"),
            entry.get("home_goals"),
            entry.get("away_goals"),
            entry.get("xpoints_home"),
            entry.get("xpoints_away")
        ))
        inserted += cur.rowcount

    conn.commit()
    print(f"✅ Game xGoals entries inserted: {inserted}")


def insert_periods(conn, data):
    print("📥 insert_periods called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        cur.execute('''
            INSERT OR IGNORE INTO Periods (
                game_id, period_id,
                min_expanded_minute, min_game_minute,
                max_expanded_minute, max_game_minute
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            entry.get("game_id"),
            entry.get("period_id"),
            entry.get("min_expanded_minute"),
            entry.get("min_game_minute"),
            entry.get("max_expanded_minute"),
            entry.get("max_game_minute")
        ))
        inserted += cur.rowcount

    conn.commit()
    print(f"✅ Periods inserted: {inserted}")

def insert_shots(conn, data):
    print("📥 insert_shots called")
    cur = conn.cursor()
    inserted = 0

    for entry in data:
        try:
            cur.execute('''
                INSERT OR IGNORE INTO Shots (
                    game_id, period_id, expanded_minute, game_minute,
                    team_id, shooter_player_id, assist_player_id,
                    shot_location_x, shot_location_y,
                    shot_end_location_x, shot_end_location_y,
                    distance_from_goal, distance_from_goal_yds,
                    blocked, blocked_x, blocked_y, goal, own_goal,
                    home_score, away_score, shot_xg, shot_psxg,
                    head, assist_through_ball, assist_cross,
                    pattern_of_play, shot_order
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                entry.get("game_id"),
                entry.get("period_id"),
                entry.get("expanded_minute"),
                entry.get("game_minute"),
                entry.get("team_id"),
                entry.get("shooter_player_id"),
                entry.get("assist_player_id"),
                entry.get("shot_location_x"),
                entry.get("shot_location_y"),
                entry.get("shot_end_location_x"),
                entry.get("shot_end_location_y"),
                entry.get("distance_from_goal"),
                entry.get("distance_from_goal_yds"),
                entry.get("blocked"),
                entry.get("blocked_x"),
                entry.get("blocked_y"),
                entry.get("goal"),
                entry.get("own_goal"),
                entry.get("home_score"),
                entry.get("away_score"),
                entry.get("shot_xg"),
                entry.get("shot_psxg"),
                entry.get("head"),
                entry.get("assist_through_ball"),
                entry.get("assist_cross"),
                entry.get("pattern_of_play"),
                entry.get("shot_order")
            ))
            inserted += cur.rowcount
        except Exception as e:
            print(f"❌ Shot insert failed: {e}")

    conn.commit()
    print(f"✅ Shots entries inserted: {inserted}")


