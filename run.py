# run.py

from config.config_loader import ASA_API_BASE, DB_NAME
from core.fetch_nwsl_data import (
    init_seasons_data,
    fetch_team_data,
    fetch_player_data,
    fetch_games_data
)

def main():
    print("⚽ Initializing NWSL Fan Engagement DB...")

    # Step 1: Create Seasons table
    init_seasons_data(firstseason=2013, thisyear=2025)

    # Step 2: Fetch static data
    fetch_team_data(f"{ASA_API_BASE}/teams")
    fetch_player_data(f"{ASA_API_BASE}/players")

    # Step 3: Loop through seasons and fetch games
    for year in range(2015, 2026):  # 2026 is exclusive
        print(f"📥 Fetching game data for {year}")
        fetch_games_data(f"{ASA_API_BASE}/games", str(year))

    print(f"✅ Done. Data saved to {DB_NAME}")

if __name__ == "__main__":
    main()
