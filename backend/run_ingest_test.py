"""
Test ingestion: creates schema, inserts teams and one season of games.
Run this before the full ingestion to verify the Postgres connection works.
"""
import os
from dotenv import load_dotenv
from core.db import get_connection, create_schema
from core.asa_client import fetch_data, insert_teams, insert_games

load_dotenv()
ASA_API_BASE = os.getenv("ASA_API_BASE")

def run_test_ingestion():
    print("🔗 Connecting to Postgres...")
    conn = get_connection()
    print("✅ Connected.")

    print("📐 Creating schema...")
    create_schema(conn)

    print("⚽ Fetching teams...")
    team_data = fetch_data(f"{ASA_API_BASE}/teams")
    print(f"   Got {len(team_data)} teams")
    insert_teams(conn, team_data)

    print("🎮 Fetching games for 2024...")
    game_data = fetch_data(f"{ASA_API_BASE}/games?season_name=2024")
    print(f"   Got {len(game_data)} games")
    insert_games(conn, game_data)

    conn.close()
    print("\n✅ Test ingestion complete. Check Supabase to confirm data landed.")

if __name__ == "__main__":
    run_test_ingestion()
