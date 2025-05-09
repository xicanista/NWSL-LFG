import os
from dotenv import load_dotenv

from core.db import get_connection, create_schema
from core.fetch_nwsl_data import init_seasons_data
from core.asa_client import (
    fetch_data,
    insert_teams,
    insert_players,
    insert_games,
    insert_managers,
    insert_referees,
    insert_stadiums,
    insert_player_xgoals_gk,
    insert_player_goals_added
)

from core.logger import get_logger
logger = get_logger(__name__)

load_dotenv()
ASA_API_BASE = os.getenv("ASA_API_BASE")

if not ASA_API_BASE:
    raise ValueError("ASA_API_BASE is not set in .env")

def run_ingestion():
    conn = get_connection()
    create_schema(conn)

    # Insert seasons
    init_seasons_data(conn, 2013, 2025)

    # Fetch and insert teams
    team_data = fetch_data(f"{ASA_API_BASE}/teams")
    insert_teams(conn, team_data)

    player_data = fetch_data(f"{ASA_API_BASE}/players")
    print(f"👥 Players fetched: {len(player_data)}")
    insert_players(conn, player_data)

    for year in range(2015, 2026):
        game_data = fetch_data(f"{ASA_API_BASE}/games?season_name={year}")
        print(f"🎮 Games for {year}: {len(game_data)}")
        insert_games(conn, game_data)

    referee_data = fetch_data(f"{ASA_API_BASE}/referees")
    insert_referees(conn, referee_data)

    manager_data = fetch_data(f"{ASA_API_BASE}/managers")
    insert_managers(conn, manager_data)

    stadium_data = fetch_data(f"{ASA_API_BASE}/stadia")
    insert_stadiums(conn, stadium_data)

    player_xgk_data = fetch_data(f"{ASA_API_BASE}/goalkeepers/xgoals")
    print(f"📊 xGk data fetched: {len(player_xgk_data)}")
    insert_player_xgoals_gk(conn, player_xgk_data)

    goals_added_data = fetch_data(f"{ASA_API_BASE}/players/goals-added")
    print(f"📊 goals_added data fetched: {len(goals_added_data)}")
    insert_player_goals_added(conn, goals_added_data)

    conn.close()

print("git is being difficult")

