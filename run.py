from core.logger import get_logger
from core.ingest import run_ingestion

logger = get_logger(__name__)

def main():
    installdb = input("Would you like to set up the database? (Y/N) ")
    if installdb in ("Y", "Yes"):
        logger.info("⚽ Starting NWSL data ingestion...")
        run_ingestion()
        logger.info("✅ Ingestion complete.")
    else:
        print("The database will not be set-up.")

if __name__ == "__main__":
    main()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import sqlite3

app = FastAPI()

# Enable CORS for your React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request model
class FavoritesRequest(BaseModel):
    favorites: List[str]

# SQLite helper
def get_db_connection():
    return sqlite3.connect('nwslfandomdb1.sqlite')

# POST /api/favorites - Save to SQLite
@app.post("/api/favorites")
def save_favorites(request: FavoritesRequest):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM favorites")  # Clear all first
    for team_id in request.favorites:
        cur.execute("INSERT INTO favorites (team_id) VALUES (?)", (team_id,))
    conn.commit()
    conn.close()
    return {"favorites": request.favorites}

# GET /api/favorites - Read from SQLite
@app.get("/api/favorites")
def get_favorites():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM favorites")
    rows = cur.fetchall()
    conn.close()
    return {"favorites": [row[0] for row in rows]}

@app.get("/api/teams")
def get_all_teams():
    # ADD CODE HERE
    return {
        "teams": [
            {"id": "por", "name": "Portland Thorns"},
            {"id": "sd", "name": "San Diego Wave"},
            {"id": "kc", "name": "Kansas City Current"}
        ]
    }

@app.get("/api/player/{firstname}-{lastname}")
def get_player_details(firstname: str, lastname: str):
    print(f"Received request for player: {firstname} {lastname}")
    # Example player: Sophia Smith
    if firstname.lower() == "sophia" and lastname.lower() == "smith":
        return {
            "full_name": "Sophia Smith",
            "position": "Forward",
            "nationality": "USA",
            "image_url": "https://example.com/sophia-smith.jpg",
            "stats": {
                "goals": 12,
                "assists": 5,
                "minutes_played": 980
            },
            "merch": [
                {
                    "name": "Sophia Smith Home Jersey",
                    "image_url": "https://example.com/home-jersey.jpg",
                    "buy_link": "https://shop.example.com/home-jersey"
                },
                {
                    "name": "Sophia Smith Away Jersey",
                    "image_url": "https://example.com/away-jersey.jpg",
                    "buy_link": "https://shop.example.com/away-jersey"
                }
            ]
        }

    return {"error": "Player not found"}, 404

