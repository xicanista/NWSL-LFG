import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";

export default function TeamPage() {
  const { teamId } = useParams();
  const [team, setTeam] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch(`http://localhost:8000/api/team/${teamId}`)
      .then(res => res.json())
      .then(data => {
        if (data.error) setError(data.error);
        else setTeam(data);
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [teamId]);

  if (loading) return <p style={{ padding: "2rem" }}>Loading team...</p>;
  if (error) return <p style={{ padding: "2rem", color: "red" }}>Error: {error}</p>;

  const s = team.stats;

  return (
    <div style={{ padding: "2rem" }}>
      <Link to="/">← Back</Link>
      <h1>{team.name} ({team.abbreviation})</h1>

      {s && Object.keys(s).length > 0 && (
        <>
          <h2>All-Time Stats</h2>
          <table style={{ borderCollapse: "collapse", marginBottom: "2rem" }}>
            <tbody>
              {[
                ["Games Played", s.count_games],
                ["Goals For", s.goals_for],
                ["Goals Against", s.goals_against],
                ["Goal Difference", s.goal_difference],
                ["xGoals For", s.xgoals_for],
                ["xGoals Against", s.xgoals_against],
                ["xGoal Difference", s.xgoal_difference],
                ["Shots For", s.shots_for],
                ["Shots Against", s.shots_against],
                ["Points", s.points],
                ["xPoints", s.xpoints],
              ].map(([label, value]) => (
                <tr key={label}>
                  <td style={{ padding: "4px 16px 4px 0", fontWeight: "bold" }}>{label}</td>
                  <td style={{ padding: "4px 0" }}>{value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {team.roster && team.roster.length > 0 && (
        <>
          <h2>Current Season Roster</h2>
          <table style={{ borderCollapse: "collapse", width: "100%", marginBottom: "2rem" }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #ccc", textAlign: "left" }}>
                <th style={{ padding: "6px 12px 6px 0" }}>Player</th>
                <th style={{ padding: "6px 12px 6px 0" }}>Position</th>
                <th style={{ padding: "6px 12px 6px 0" }}>Goals</th>
                <th style={{ padding: "6px 12px 6px 0" }}>Assists</th>
                <th style={{ padding: "6px 12px 6px 0" }}>xGoals</th>
                <th style={{ padding: "6px 12px 6px 0" }}>Minutes</th>
              </tr>
            </thead>
            <tbody>
              {team.roster.map(p => (
                <tr key={p.player_id} style={{ borderBottom: "1px solid #eee" }}>
                  <td style={{ padding: "6px 12px 6px 0" }}>
                    <Link to={`/player/${p.player_name.toLowerCase().replace(" ", "-")}`}>
                      {p.player_name}
                    </Link>
                  </td>
                  <td style={{ padding: "6px 12px 6px 0" }}>{p.position || "—"}</td>
                  <td style={{ padding: "6px 12px 6px 0" }}>{p.goals ?? "—"}</td>
                  <td style={{ padding: "6px 12px 6px 0" }}>{p.assists ?? "—"}</td>
                  <td style={{ padding: "6px 12px 6px 0" }}>{p.xgoals ?? "—"}</td>
                  <td style={{ padding: "6px 12px 6px 0" }}>{p.minutes_played ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <h2>Recent Games</h2>
      {team.recent_games.length === 0 ? (
        <p>No games found.</p>
      ) : (
        <table style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #ccc", textAlign: "left" }}>
              <th style={{ padding: "6px 12px 6px 0" }}>Date</th>
              <th style={{ padding: "6px 12px 6px 0" }}>Home</th>
              <th style={{ padding: "6px 12px 6px 0" }}>Score</th>
              <th style={{ padding: "6px 12px 6px 0" }}>Away</th>
              <th style={{ padding: "6px 12px 6px 0" }}>Season</th>
              <th style={{ padding: "6px 12px 6px 0" }}>Attendance</th>
            </tr>
          </thead>
          <tbody>
            {team.recent_games.map(g => (
              <tr key={g.game_id} style={{ borderBottom: "1px solid #eee" }}>
                <td style={{ padding: "6px 12px 6px 0" }}>{g.date}</td>
                <td style={{ padding: "6px 12px 6px 0", fontWeight: g.home_team_id === teamId ? "bold" : "normal" }}>{g.home_team_name}</td>
                <td style={{ padding: "6px 12px 6px 0" }}>{g.home_score} – {g.away_score}</td>
                <td style={{ padding: "6px 12px 6px 0", fontWeight: g.away_team_id === teamId ? "bold" : "normal" }}>{g.away_team_name}</td>
                <td style={{ padding: "6px 12px 6px 0" }}>{g.season}</td>
                <td style={{ padding: "6px 12px 6px 0" }}>{g.attendance?.toLocaleString() ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
