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

@app.get("/api/recent-matches")
def get_recent_matches():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT g.game_id, g.date_time_utc, g.home_score, g.away_score,
               g.home_team_id, g.away_team_id, g.season_name, g.attendance,
               ht.name AS home_team_name, at.name AS away_team_name
        FROM Games g
        JOIN Teams ht ON g.home_team_id = ht.id
        JOIN Teams at ON g.away_team_id = at.id
        ORDER BY g.date_time_utc DESC
        LIMIT 8
    """)
    rows = cur.fetchall()
    conn.close()
    return {"matches": [
        {
            "game_id": r[0],
            "date": r[1].strftime("%b %d, %Y") if r[1] else None,
            "home_score": r[2], "away_score": r[3],
            "home_team_id": r[4], "away_team_id": r[5],
            "season": r[6], "attendance": r[7],
            "home_team_name": r[8], "away_team_name": r[9],
        }
        for r in rows
    ]}

@app.get("/api/team/{team_id}")
def get_team(team_id: str):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, name, short_name, abbreviation FROM Teams WHERE id = %s", (team_id,))
    team = cur.fetchone()
    if not team:
        conn.close()
        return {"error": "Team not found"}

    cur.execute("SELECT * FROM TeamXGoals WHERE team_id = %s", (team_id,))
    xg = cur.fetchone()
    xg_stats = {}
    if xg:
        xg_stats = {
            "count_games": xg[1], "shots_for": xg[2], "shots_against": xg[3],
            "goals_for": xg[4], "goals_against": xg[5], "goal_difference": xg[6],
            "xgoals_for": round(xg[7], 2), "xgoals_against": round(xg[8], 2),
            "xgoal_difference": round(xg[9], 2), "points": xg[11], "xpoints": round(xg[12], 2),
        }

    cur.execute("""
        SELECT g.game_id, g.date_time_utc, g.home_score, g.away_score,
               g.home_team_id, g.away_team_id, g.season_name, g.attendance, g.status,
               ht.name AS home_team_name, at.name AS away_team_name
        FROM Games g
        JOIN Teams ht ON g.home_team_id = ht.id
        JOIN Teams at ON g.away_team_id = at.id
        WHERE g.home_team_id = %s OR g.away_team_id = %s
        ORDER BY g.date_time_utc DESC
        LIMIT 20
    """, (team_id, team_id))
    game_rows = cur.fetchall()
    games = [
        {
            "game_id": r[0],
            "date": r[1].strftime("%Y-%m-%d") if r[1] else None,
            "home_score": r[2], "away_score": r[3],
            "home_team_id": r[4], "away_team_id": r[5],
            "season": r[6], "attendance": r[7], "status": r[8],
            "home_team_name": r[9], "away_team_name": r[10],
        }
        for r in game_rows
    ]

    cur.execute("""
        SELECT p.player_name, s.player_id, s.general_position, s.minutes_played,
               s.goals, s.xgoals, s.primary_assists
        FROM PlayerSeasonStats s
        JOIN Players p ON s.player_id = p.player_id
        WHERE s.team_id = %s AND s.season_name = (
            SELECT MAX(season_name) FROM PlayerSeasonStats WHERE team_id = %s
        )
        ORDER BY s.minutes_played DESC
    """, (team_id, team_id))
    roster_rows = cur.fetchall()
    roster = [
        {
            "player_name": r[0], "player_id": r[1], "position": r[2],
            "minutes_played": r[3], "goals": r[4],
            "xgoals": round(r[5], 2) if r[5] else None,
            "assists": r[6],
        }
        for r in roster_rows
    ]

    conn.close()
    return {
        "id": team[0], "name": team[1], "short_name": team[2], "abbreviation": team[3],
        "stats": xg_stats,
        "recent_games": games,
        "roster": roster,
    }

@app.get("/api/player/{firstname}-{lastname}")
def get_player_details(firstname: str, lastname: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT player_id, player_name, primary_general_position, nationality, birth_date, height_ft, height_in FROM Players WHERE LOWER(player_name) = LOWER(%s)",
        (f"{firstname} {lastname}",)
    )
    player = cur.fetchone()

    if not player:
        conn.close()
        return {"error": "Player not found"}

    player_id, player_name, position, nationality, birth_date, height_ft, height_in = player

    cur.execute("""
        SELECT s.season_name, s.team_id, t.name, s.general_position,
               s.goals, s.shots, s.shots_on_target, s.xgoals,
               s.primary_assists, s.minutes_played
        FROM PlayerSeasonStats s
        LEFT JOIN Teams t ON s.team_id = t.id
        WHERE s.player_id = %s
        ORDER BY s.season_name DESC
        LIMIT 1
    """, (player_id,))
    stats_row = cur.fetchone()

    stats = {}
    current_team_id = None
    if stats_row:
        current_team_id = stats_row[1]
        stats = {
            "season": stats_row[0],
            "team": stats_row[2] or stats_row[1],
            "position": stats_row[3],
            "goals": stats_row[4],
            "shots": stats_row[5],
            "shots_on_target": stats_row[6],
            "xgoals": round(stats_row[7], 2) if stats_row[7] else None,
            "assists": stats_row[8],
            "minutes_played": stats_row[9],
        }

    recent_games = []
    if current_team_id:
        cur.execute("""
            SELECT g.date_time_utc, g.home_score, g.away_score,
                   g.home_team_id, g.away_team_id,
                   ht.name, at.name
            FROM Games g
            JOIN Teams ht ON g.home_team_id = ht.id
            JOIN Teams at ON g.away_team_id = at.id
            WHERE (g.home_team_id = %s OR g.away_team_id = %s)
            AND g.season_name = %s
            ORDER BY g.date_time_utc DESC
            LIMIT 5
        """, (current_team_id, current_team_id, stats_row[0]))
        for r in cur.fetchall():
            is_home = r[3] == current_team_id
            team_score = r[1] if is_home else r[2]
            opp_score = r[2] if is_home else r[1]
            opp_name = r[6] if is_home else r[5]
            if team_score > opp_score: result = "W"
            elif team_score < opp_score: result = "L"
            else: result = "D"
            recent_games.append({
                "date": r[0].strftime("%b %d") if r[0] else None,
                "opponent": opp_name,
                "score": f"{team_score}–{opp_score}",
                "result": result,
                "home": is_home,
            })

    conn.close()
    return {
        "full_name": player_name,
        "position": position,
        "nationality": nationality,
        "birth_date": str(birth_date) if birth_date else None,
        "height": f"{height_ft}'{height_in}\"" if height_ft else None,
        "stats": stats,
        "recent_games": recent_games,
    }
