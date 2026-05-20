"""
Ingest per-season player stats into PlayerSeasonStats table.
Creates the table if it doesn't exist, then fetches xgoals data
for each season from the ASA API.
"""
import os
from dotenv import load_dotenv
from core.db import get_connection, create_schema
from core.asa_client import fetch_data, insert_player_season_stats

load_dotenv()
ASA_API_BASE = os.getenv("ASA_API_BASE")

conn = get_connection()

print("📐 Ensuring schema is up to date...")
create_schema(conn)

for year in range(2016, 2027):
    url = f"{ASA_API_BASE}/players/xgoals?season_name={year}"
    data = fetch_data(url)
    print(f"📊 Season {year}: {len(data)} players fetched")
    if data:
        insert_player_season_stats(conn, data, str(year))

conn.close()
print("\n✅ Season stats ingestion complete.")
