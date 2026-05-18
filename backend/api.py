import os
import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FavoritesRequest(BaseModel):
    favorites: List[str]

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

@app.post("/api/favorites")
def save_favorites(request: FavoritesRequest):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM favorites")
    for team_id in request.favorites:
        cur.execute("INSERT INTO favorites (team_id) VALUES (%s)", (team_id,))
    conn.commit()
    conn.close()
    return {"favorites": request.favorites}

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
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM Teams ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return {"teams": [{"id": row[0], "name": row[1]} for row in rows]}

@app.get("/api/player/{firstname}-{lastname}")
def get_player_details(firstname: str, lastname: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT player_id, player_name, primary_general_position, nationality FROM Players WHERE LOWER(player_name) = LOWER(%s)",
        (f"{firstname} {lastname}",)
    )
    player = cur.fetchone()

    if not player:
        conn.close()
        return {"error": "Player not found"}

    player_id, player_name, position, nationality = player

    cur.execute(
        "SELECT goals, shots, shots_on_target, xgoals, primary_assists, minutes_played FROM PlayerXGoals WHERE player_id = %s",
        (player_id,)
    )
    stats_row = cur.fetchone()
    conn.close()

    stats = {}
    if stats_row:
        stats = {
            "goals": stats_row[0],
            "shots": stats_row[1],
            "shots_on_target": stats_row[2],
            "xgoals": stats_row[3],
            "assists": stats_row[4],
            "minutes_played": stats_row[5],
        }

    return {
        "full_name": player_name,
        "position": position,
        "nationality": nationality,
        "stats": stats,
        "merch": []
    }
