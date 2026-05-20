import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";

const STAT_EXPLANATIONS = {
  count_games:    { label: "Games Played", explain: "Total games played across all seasons in the database." },
  goals_for:      { label: "Goals Scored", explain: "Total goals your team has scored. The most obvious measure of attacking output." },
  goals_against:  { label: "Goals Conceded", explain: "Total goals your team has let in. Lower is better — this reflects defensive solidity." },
  goal_difference:{ label: "Goal Difference", explain: "Goals scored minus goals conceded. Positive means your team scores more than it concedes — a good sign of overall dominance." },
  xgoals_for:     { label: "xGoals For (xG)", explain: "Expected goals — how many goals your team *should* have scored based on the quality of shots taken. Higher than actual goals = unlucky finishing. Lower = clinical." },
  xgoals_against: { label: "xGoals Against", explain: "How many goals your team *should* have conceded based on shots faced. Lower than actual goals conceded = your goalkeeper has been unlucky." },
  xgoal_difference:{ label: "xGoal Difference", explain: "The gap between expected goals scored and conceded. Positive means you've been the better team on balance, regardless of the scoreline." },
  shots_for:      { label: "Shots For", explain: "Total shots your team has taken. High volume often reflects offensive pressure and dominance." },
  shots_against:  { label: "Shots Against", explain: "Total shots your team has faced. Lower means your defense is limiting opponents' chances." },
  points:         { label: "Points", explain: "League points earned (3 for a win, 1 for a draw, 0 for a loss). The standard measure of league performance." },
  xpoints:        { label: "xPoints", explain: "Expected points based on xG — what your points tally *should* be if results matched performance quality. Higher than actual = you've been unlucky; lower = you've punched above your weight." },
};

const STAT_ORDER = [
  "count_games", "goals_for", "goals_against", "goal_difference",
  "xgoals_for", "xgoals_against", "xgoal_difference",
  "shots_for", "shots_against", "points", "xpoints"
];

function StatRow({ statKey, value }) {
  const [open, setOpen] = useState(false);
  const meta = STAT_EXPLANATIONS[statKey];
  if (!meta) return null;
  return (
    <tr style={{ borderBottom: "1px solid #eee" }}>
      <td style={{ padding: "8px 12px 8px 0", fontWeight: "600", width: "200px" }}>
        {meta.label}
        <button
          onClick={() => setOpen(!open)}
          style={{
            marginLeft: "6px", background: "none", border: "1px solid #c4b5fd",
            borderRadius: "50%", width: "18px", height: "18px", cursor: "pointer",
            fontSize: "11px", color: "#7c3aed", lineHeight: "16px", padding: 0
          }}
          title="What does this mean?"
        >?</button>
      </td>
      <td style={{ padding: "8px 0" }}>
        <div>{value}</div>
        {open && (
          <div style={{
            marginTop: "4px", fontSize: "0.85rem", color: "#555",
            backgroundColor: "#faf5ff", padding: "6px 10px",
            borderRadius: "6px", borderLeft: "3px solid #7c3aed"
          }}>
            {meta.explain}
          </div>
        )}
      </td>
    </tr>
  );
}

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
    <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem" }}>
      <Link to="/" style={{ color: "#7c3aed", textDecoration: "none" }}>← Back</Link>
      <h1 style={{ marginTop: "1rem" }}>{team.name} <span style={{ color: "#888", fontWeight: "normal" }}>({team.abbreviation})</span></h1>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem", marginTop: "2rem" }}>

        {/* All-time Stats */}
        {s && Object.keys(s).length > 0 && (
          <section>
            <h2>All-Time Stats</h2>
            <p style={{ fontSize: "0.85rem", color: "#888", marginTop: "-0.5rem" }}>
              Click the <span style={{ color: "#7c3aed" }}>?</span> next to any stat for a plain-English explanation.
            </p>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <tbody>
                {STAT_ORDER.map(key => s[key] != null && (
                  <StatRow key={key} statKey={key} value={s[key]} />
                ))}
              </tbody>
            </table>
          </section>
        )}

        {/* Team News placeholder */}
        <section>
          <h2>Team News</h2>
          <div style={{ display: "grid", gap: "0.75rem" }}>
            {[
              "Match preview: What to expect this weekend",
              "Injury update ahead of next fixture",
              "Post-match: Three things we learned",
            ].map((headline, i) => (
              <div key={i} style={{
                padding: "1rem", border: "1px solid #e0e0e0",
                borderRadius: "8px", backgroundColor: "#fafafa"
              }}>
                <div style={{
                  fontSize: "0.75rem", fontWeight: "bold", color: "#7c3aed",
                  marginBottom: "0.25rem"
                }}>COMING SOON</div>
                <p style={{ margin: 0, color: "#555", fontStyle: "italic" }}>{headline}</p>
              </div>
            ))}
          </div>
        </section>
      </div>

      {/* Current Season Roster */}
      {team.roster && team.roster.length > 0 && (
        <section style={{ marginTop: "2rem" }}>
          <h2>Current Season Roster</h2>
          <table style={{ borderCollapse: "collapse", width: "100%" }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #ccc", textAlign: "left" }}>
                <th style={{ padding: "8px 12px 8px 0" }}>Player</th>
                <th style={{ padding: "8px 12px 8px 0" }}>Position</th>
                <th style={{ padding: "8px 12px 8px 0" }}>Goals</th>
                <th style={{ padding: "8px 12px 8px 0" }}>Assists</th>
                <th style={{ padding: "8px 12px 8px 0" }}>xGoals</th>
                <th style={{ padding: "8px 12px 8px 0" }}>Minutes</th>
              </tr>
            </thead>
            <tbody>
              {team.roster.map(p => (
                <tr key={p.player_id} style={{ borderBottom: "1px solid #eee" }}>
                  <td style={{ padding: "8px 12px 8px 0" }}>
                    <Link
                      to={`/player/${p.player_name.toLowerCase().replace(/ /g, "-")}`}
                      style={{ color: "#7c3aed", textDecoration: "none" }}
                    >
                      {p.player_name}
                    </Link>
                  </td>
                  <td style={{ padding: "8px 12px 8px 0" }}>{p.position || "—"}</td>
                  <td style={{ padding: "8px 12px 8px 0" }}>{p.goals ?? "—"}</td>
                  <td style={{ padding: "8px 12px 8px 0" }}>{p.assists ?? "—"}</td>
                  <td style={{ padding: "8px 12px 8px 0" }}>{p.xgoals ?? "—"}</td>
                  <td style={{ padding: "8px 12px 8px 0" }}>{p.minutes_played ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      {/* Recent Results */}
      {team.recent_games && team.recent_games.length > 0 && (
        <section style={{ marginTop: "2rem" }}>
          <h2>Recent Results</h2>
          <div style={{ display: "grid", gap: "0.5rem" }}>
            {team.recent_games.map(g => (
              <div key={g.game_id} style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                padding: "0.75rem 1rem", border: "1px solid #e0e0e0",
                borderRadius: "8px", backgroundColor: "#fafafa"
              }}>
                <div style={{ flex: 1, textAlign: "right" }}>
                  <Link to={`/team/${g.home_team_id}`} style={{
                    fontWeight: g.home_team_id === teamId ? "bold" : "normal",
                    textDecoration: "none", color: "#222"
                  }}>
                    {g.home_team_name}
                  </Link>
                </div>
                <div style={{ padding: "0 1rem", fontWeight: "bold", minWidth: "80px", textAlign: "center" }}>
                  {g.home_score} – {g.away_score}
                </div>
                <div style={{ flex: 1 }}>
                  <Link to={`/team/${g.away_team_id}`} style={{
                    fontWeight: g.away_team_id === teamId ? "bold" : "normal",
                    textDecoration: "none", color: "#222"
                  }}>
                    {g.away_team_name}
                  </Link>
                </div>
                <div style={{ color: "#888", fontSize: "0.85rem", marginLeft: "1rem", minWidth: "90px", textAlign: "right" }}>
                  {g.date}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
