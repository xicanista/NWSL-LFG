"""
Resume ingestion from PlayerXGoals onwards after FK constraint fix.
Run this once after the initial ingestion failed on PlayerXGoals.
"""
import os
from dotenv import load_dotenv
from core.db import get_connection
from core.asa_client import (
    fetch_data,
    insert_player_xgoals,
    insert_player_xgoals_gk,
    insert_team_xgoals,
    insert_game_xgoals,
    insert_periods,
    insert_shots,
)

load_dotenv()
ASA_API_BASE = os.getenv("ASA_API_BASE")

conn = get_connection()
cur = conn.cursor()

print("🔧 Dropping and recreating PlayerXGoals without FK constraints...")
cur.execute("DROP TABLE IF EXISTS PlayerXGoals")
cur.execute('''
    CREATE TABLE PlayerXGoals (
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
        xpoints_added FLOAT
    )
''')
conn.commit()
print("✅ PlayerXGoals recreated.")

players_xgoals_data = fetch_data(f"{ASA_API_BASE}/players/xgoals")
print(f"📊 Player xGoals data fetched: {len(players_xgoals_data)}")
insert_player_xgoals(conn, players_xgoals_data)

goalkeepers_xgoals_data = fetch_data(f"{ASA_API_BASE}/goalkeepers/xgoals")
print(f"🧤 Goalkeeper xGoals data fetched: {len(goalkeepers_xgoals_data)}")
insert_player_xgoals_gk(conn, goalkeepers_xgoals_data)

games_xgoals_data = fetch_data(f"{ASA_API_BASE}/games/xgoals")
print(f"📆 Game xGoals data fetched: {len(games_xgoals_data)}")
insert_game_xgoals(conn, games_xgoals_data)

periods_data = fetch_data(f"{ASA_API_BASE}/games/periods")
print(f"🕓 Periods data fetched: {len(periods_data)}")
insert_periods(conn, periods_data)

shots_data = fetch_data(f"{ASA_API_BASE}/games/shots")
print(f"🎯 Shots data fetched: {len(shots_data)}")
insert_shots(conn, shots_data)

conn.close()
print("\n✅ Resume ingestion complete.")
