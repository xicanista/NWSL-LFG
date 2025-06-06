import os
from dotenv import load_dotenv

from core.db import get_connection, create_schema
#from core.fetch_nwsl_data import init_seasons_data
from core.db import init_seasons_data
from core.asa_client import (
    fetch_data,
    insert_teams,
    insert_players,
    insert_games,
    insert_managers,
    insert_referees,
    insert_stadiums,
    insert_player_goals_added,
    insert_player_xpass,
    insert_periods,
    insert_player_xgoals,
    insert_player_xgoals_gk,
    insert_team_xgoals,
    insert_game_xgoals,
    insert_shots
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

    goals_added_data = fetch_data(f"{ASA_API_BASE}/players/goals-added")
    print(f"📊 goals_added data fetched: {len(goals_added_data)}")
    insert_player_goals_added(conn, goals_added_data)

    player_xpass_data = fetch_data(f"{ASA_API_BASE}/players/xpass")
    print(f"📊 xPass data fetched: {len(player_xpass_data)}")
    insert_player_xpass(conn, player_xpass_data)

    teams_xgoals_data = fetch_data(f"{ASA_API_BASE}/teams/xgoals")
    print(f"📊 Team xGoals data fetched: {len(teams_xgoals_data)}")
    insert_team_xgoals(conn, teams_xgoals_data)

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

