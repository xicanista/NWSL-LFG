import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def init_seasons_data(conn, firstseason, thisyear):
    cur = conn.cursor()
    count = 1
    while thisyear >= firstseason:
        cur.execute(
            'INSERT INTO Seasons (season_id, season_name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
            (count, str(thisyear))
        )
        thisyear -= 1
        count += 1
    conn.commit()

def get_connection():
    print(f"🔗 Connecting to Postgres DB")
    return psycopg2.connect(DATABASE_URL)

def create_schema(conn):
    cur = conn.cursor()

    def safe_execute(sql, name):
        try:
            cur.execute(sql)
            print(f"✅ Created table: {name}")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"❌ Error creating {name}: {e}")

    tables = {
        "Teams": '''
            CREATE TABLE IF NOT EXISTS Teams (
                id TEXT NOT NULL PRIMARY KEY,
                name TEXT UNIQUE,
                short_name TEXT UNIQUE,
                abbreviation TEXT UNIQUE
            )
        ''',

        "Players": '''
            CREATE TABLE IF NOT EXISTS Players (
                player_id TEXT NOT NULL PRIMARY KEY,
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
            )
        ''',

        "Seasons": '''
            CREATE TABLE IF NOT EXISTS Seasons (
                season_id INTEGER NOT NULL PRIMARY KEY,
                season_name TEXT UNIQUE
            )
        ''',

        "Games": '''
            CREATE TABLE IF NOT EXISTS Games (
                game_id TEXT NOT NULL PRIMARY KEY,
                date_time_utc TIMESTAMP,
                home_score INTEGER,
                away_score INTEGER,
                home_team_id TEXT,
                away_team_id TEXT,
                referee_id TEXT,
                stadium_id TEXT,
                home_manager_id TEXT,
                away_manager_id TEXT,
                expanded_minutes INTEGER,
                season_name TEXT,
                matchday TEXT,
                attendance INTEGER,
                knockout_game TEXT,
                status TEXT,
                last_updated_utc TIMESTAMP
            )
        ''',

        "Referees": '''
            CREATE TABLE IF NOT EXISTS Referees (
                referee_id TEXT NOT NULL PRIMARY KEY,
                referee_name TEXT,
                nationality TEXT
            )
        ''',

        "Managers": '''
            CREATE TABLE IF NOT EXISTS Managers (
                manager_id TEXT NOT NULL PRIMARY KEY,
                manager_name TEXT,
                nationality TEXT
            )
        ''',

        "Stadiums": '''
            CREATE TABLE IF NOT EXISTS Stadiums (
                stadium_id TEXT NOT NULL PRIMARY KEY,
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
            )
        ''',

        "PlayerXGoalsGK": '''
            CREATE TABLE IF NOT EXISTS PlayerXGoalsGK (
                id SERIAL PRIMARY KEY,
                player_id TEXT,
                team_id TEXT,
                minutes_played INTEGER,
                shots_faced INTEGER,
                goals_conceded INTEGER,
                saves INTEGER,
                share_headed_shots REAL,
                xgoals_gk_faced REAL,
                goals_minus_xgoals_gk REAL,
                goals_divided_by_xgoals_gk REAL
            )
        ''',

        "PlayerGoalsAdded": '''
            CREATE TABLE IF NOT EXISTS PlayerGoalsAdded (
                player_id TEXT PRIMARY KEY,
                team_id TEXT,
                general_position TEXT,
                minutes_played INTEGER
            )
        ''',

        "PlayerGoalsAddedActions": '''
            CREATE TABLE IF NOT EXISTS PlayerGoalsAddedActions (
                player_id TEXT,
                action_type TEXT,
                goals_added_raw REAL,
                goals_added_above_avg REAL,
                count_actions INTEGER,
                PRIMARY KEY (player_id, action_type),
                FOREIGN KEY (player_id) REFERENCES PlayerGoalsAdded(player_id)
            )
        ''',

        "favorites": '''
            CREATE TABLE IF NOT EXISTS favorites (
                id SERIAL PRIMARY KEY,
                team_id TEXT NOT NULL UNIQUE
            )
        ''',

        "GameFlow": '''
            CREATE TABLE IF NOT EXISTS GameFlow (
                game_id TEXT,
                period_id INTEGER,
                expanded_minute INTEGER,
                home_team_id TEXT,
                home_team_value FLOAT,
                away_team_id TEXT,
                away_team_value FLOAT
            )
        ''',

        "Shots": '''
            CREATE TABLE IF NOT EXISTS Shots (
                game_id TEXT,
                period_id INTEGER,
                expanded_minute INTEGER,
                game_minute INTEGER,
                team_id TEXT,
                shooter_player_id TEXT,
                assist_player_id TEXT,
                shot_location_x FLOAT,
                shot_location_y FLOAT,
                shot_end_location_x FLOAT,
                shot_end_location_y FLOAT,
                distance_from_goal FLOAT,
                distance_from_goal_yds FLOAT,
                blocked INTEGER,
                blocked_x FLOAT,
                blocked_y FLOAT,
                goal INTEGER,
                own_goal INTEGER,
                home_score INTEGER,
                away_score INTEGER,
                shot_xg FLOAT,
                shot_psxg FLOAT,
                head INTEGER,
                assist_through_ball INTEGER,
                assist_cross INTEGER,
                pattern_of_play TEXT,
                shot_order INTEGER
            )
        ''',

        "Periods": '''
            CREATE TABLE IF NOT EXISTS Periods (
                game_id TEXT,
                period_id INTEGER,
                min_expanded_minute INTEGER,
                min_game_minute INTEGER,
                max_expanded_minute INTEGER,
                max_game_minute INTEGER,
                PRIMARY KEY (game_id, period_id),
                FOREIGN KEY (game_id) REFERENCES Games(game_id)
            )
        ''',

        "TeamXGoals": '''
            CREATE TABLE IF NOT EXISTS TeamXGoals (
                team_id TEXT PRIMARY KEY,
                count_games INTEGER,
                shots_for INTEGER,
                shots_against INTEGER,
                goals_for INTEGER,
                goals_against INTEGER,
                goal_difference INTEGER,
                xgoals_for FLOAT,
                xgoals_against FLOAT,
                xgoal_difference FLOAT,
                goal_difference_minus_xgoal_difference FLOAT,
                points INTEGER,
                xpoints FLOAT
            )
        ''',

        "PlayerXGoals": '''
            CREATE TABLE IF NOT EXISTS PlayerXGoals (
                player_id TEXT PRIMARY KEY,
                team_id TEXT,
                general_position TEXT,
                minutes_played INTEGER,
                shots INTEGER,
                shots_on_target INTEGER,
                goals INTEGER,
                xgoals FLOAT,
                xplace FLOAT,
                goals_minus_xgoals FLOAT,
                key_passes INTEGER,
                primary_assists INTEGER,
                xassists FLOAT,
                primary_assists_minus_xassists FLOAT,
                goals_plus_primary_assists INTEGER,
                xgoals_plus_xassists FLOAT,
                points_added FLOAT,
                xpoints_added FLOAT,
                FOREIGN KEY (player_id) REFERENCES Players(player_id),
                FOREIGN KEY (team_id) REFERENCES Teams(id)
            )
        ''',

        "GameXgoals": '''
            CREATE TABLE IF NOT EXISTS GameXgoals (
                game_id TEXT PRIMARY KEY,
                home_team_id TEXT,
                away_team_id TEXT,
                xgoals_home FLOAT,
                xgoals_away FLOAT,
                home_goals INTEGER,
                away_goals INTEGER,
                xpoints_home FLOAT,
                xpoints_away FLOAT
            )
        ''',

        "PlayerXpass": '''
            CREATE TABLE IF NOT EXISTS PlayerXpass (
                player_id TEXT PRIMARY KEY,
                team_id TEXT,
                general_position TEXT,
                minutes_played INTEGER,
                attempted_passes INTEGER,
                pass_completion_percentage FLOAT,
                xpass_completion_percentage FLOAT,
                passes_completed_over_expected FLOAT,
                passes_completed_over_expected_p100 FLOAT,
                avg_distance_yds FLOAT,
                avg_vertical_distance_yds FLOAT,
                share_team_touches FLOAT,
                count_games INTEGER
            )
        ''',
    }

    for name, sql in tables.items():
        safe_execute(sql, name)

    conn.commit()
    print("✅ All tables created successfully.")
