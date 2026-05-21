import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getTeamTheme } from "../teamThemes";

const STAT_EXPLANATIONS = {
  count_games:     { label: "Games Played",      explain: "Total games played across all seasons in the database." },
  goals_for:       { label: "Goals Scored",       explain: "Total goals your team has scored. The most obvious measure of attacking output." },
  goals_against:   { label: "Goals Conceded",     explain: "Total goals your team has let in. Lower is better — this reflects defensive solidity." },
  goal_difference: { label: "Goal Difference",    explain: "Goals scored minus goals conceded. Positive means your team scores more than it concedes — a good sign of overall dominance." },
  xgoals_for:      { label: "xGoals For (xG)",    explain: "Expected goals — how many goals your team *should* have scored based on shot quality. Higher than actual = unlucky finishing. Lower = clinical." },
  xgoals_against:  { label: "xGoals Against",     explain: "How many goals your team *should* have conceded based on shots faced. Lower than actual goals conceded = goalkeeper has been unlucky." },
  xgoal_difference:{ label: "xGoal Difference",   explain: "The gap between expected goals scored and conceded. Positive means you've been the better team on balance, regardless of the scoreline." },
  shots_for:       { label: "Shots For",          explain: "Total shots your team has taken. High volume often reflects offensive pressure and dominance." },
  shots_against:   { label: "Shots Against",      explain: "Total shots your team has faced. Lower means your defense is limiting opponents' chances." },
  points:          { label: "Points",             explain: "League points earned (3 for a win, 1 for a draw, 0 for a loss). The standard measure of league performance." },
  xpoints:         { label: "xPoints",            explain: "Expected points based on xG — what your points tally *should* be if results matched performance quality. Higher than actual = unlucky; lower = punched above your weight." },
};

const STAT_ORDER = [
  "count_games", "goals_for", "goals_against", "goal_difference",
  "xgoals_for", "xgoals_against", "xgoal_difference",
  "shots_for", "shots_against", "points", "xpoints",
];

const RESULT_COLORS = { W: "#16a34a", D: "#ca8a04", L: "#dc2626" };

function Card({ children, style = {} }) {
  return (
    <div style={{
      background: "#fff", border: "1px solid #e0e0e0",
      borderRadius: "12px", padding: "1.5rem", ...style
    }}>
      {children}
    </div>
  );
}

function SectionHeading({ children }) {
  return (
    <h2 style={{
      fontSize: "0.85rem", fontWeight: "700", textTransform: "uppercase",
      letterSpacing: "0.06em", color: "#888", marginBottom: "1rem", marginTop: 0
    }}>
      {children}
    </h2>
  );
}

function StatRow({ statKey, value, theme }) {
  const [open, setOpen] = useState(false);
  const meta = STAT_EXPLANATIONS[statKey];
  if (!meta) return null;
  return (
    <tr style={{ borderBottom: "1px solid #f0f0f0" }}>
      <td style={{ padding: "8px 0", color: "#555", fontSize: "0.95rem" }}>
        {meta.label}
        <button
          onClick={() => setOpen(!open)}
          style={{
            marginLeft: "5px", background: theme.light, border: "none",
            borderRadius: "50%", width: "16px", height: "16px", cursor: "pointer",
            fontSize: "10px", color: theme.accent, lineHeight: "16px", padding: 0,
            fontWeight: "700", verticalAlign: "middle"
          }}
          title="What does this mean?"
        >?</button>
        {open && (
          <div style={{
            fontSize: "0.78rem", color: theme.accent, marginTop: "2px",
            display: "block"
          }}>
            {meta.explain}
          </div>
        )}
      </td>
      <td style={{ padding: "8px 0", fontWeight: "700", textAlign: "right", fontSize: "0.95rem" }}>
        {value}
      </td>
    </tr>
  );
}

export default function TeamPage() {
  const { teamId } = useParams();
  const [team, setTeam] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const theme = getTeamTheme(teamId);

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

  // Derive summary badges from stats
  const badges = [];
  if (s?.count_games) badges.push({ label: `${s.count_games} Games Played`, style: "gray" });
  if (s?.goal_difference != null) {
    const gd = s.goal_difference;
    badges.push({
      label: `${gd > 0 ? "+" : ""}${gd} Goal Difference`,
      style: gd >= 0 ? "green" : "red"
    });
  }
  if (s?.points) badges.push({ label: `${s.points} Points`, style: "accent" });

  const badgeStyles = {
    gray:   { background: "#f3f4f6", color: "#555" },
    green:  { background: "#dcfce7", color: "#15803d" },
    red:    { background: "#fee2e2", color: "#dc2626" },
    accent: { background: theme.light, color: theme.accent },
  };

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", background: "#f5f5f5", minHeight: "100vh" }}>

      {/* Nav bar */}
      <nav style={{
        background: "#fff", borderBottom: "1px solid #e0e0e0",
        padding: "0 2rem", display: "flex", alignItems: "center",
        height: "56px", gap: "2rem"
      }}>
        <Link to="/" style={{ fontWeight: "800", fontSize: "1.1rem", color: "#111", textDecoration: "none" }}>
          ⚽ NWSL-LFG
        </Link>
        <Link to="/" style={{ textDecoration: "none", color: "#555", fontSize: "0.9rem" }}>← All Teams</Link>
      </nav>

      {/* Hero */}
      <div style={{
        background: theme.primary, color: theme.primaryText,
        padding: "2rem", display: "flex", alignItems: "center", gap: "1.5rem"
      }}>
        {/* Logo placeholder */}
        <div style={{
          width: "80px", height: "80px", borderRadius: "50%",
          background: "rgba(255,255,255,0.12)", border: `2px solid ${theme.accent}`,
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: "0.65rem", color: theme.accent, fontWeight: "700",
          textAlign: "center", flexShrink: 0, lineHeight: 1.3
        }}>
          LOGO<br />HERE
        </div>
        <div>
          <h1 style={{ fontSize: "1.8rem", fontWeight: "800", margin: 0 }}>
            {team.name}
            {team.abbreviation && (
              <span style={{ fontWeight: "400", opacity: 0.55, fontSize: "1.1rem", marginLeft: "0.5rem" }}>
                ({team.abbreviation})
              </span>
            )}
          </h1>
          <div style={{ opacity: 0.65, fontSize: "0.9rem", marginTop: "0.25rem" }}>NWSL</div>
          {badges.length > 0 && (
            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.75rem", flexWrap: "wrap" }}>
              {badges.map((b, i) => (
                <span key={i} style={{
                  padding: "3px 10px", borderRadius: "12px", fontSize: "0.8rem",
                  fontWeight: "600", ...badgeStyles[b.style]
                }}>
                  {b.label}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Page body */}
      <div style={{ maxWidth: "960px", margin: "0 auto", padding: "2rem" }}>

        {/* Row 1: Stats + News */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem", marginBottom: "2rem" }}>

          {s && Object.keys(s).length > 0 && (
            <Card>
              <SectionHeading>All-Time Stats</SectionHeading>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <tbody>
                  {STAT_ORDER.map(key => s[key] != null && (
                    <StatRow key={key} statKey={key} value={s[key]} theme={theme} />
                  ))}
                </tbody>
              </table>
            </Card>
          )}

          <Card>
            <SectionHeading>Team News</SectionHeading>
            {[
              "Match preview: What to expect this weekend",
              "Injury update ahead of next fixture",
              "Post-match: Three things we learned",
              "Player spotlight: top performer in form",
            ].map((headline, i) => (
              <div key={i} style={{
                padding: "0.75rem 0",
                borderBottom: i < 3 ? "1px solid #f0f0f0" : "none"
              }}>
                <div style={{
                  fontSize: "0.7rem", fontWeight: "700", color: theme.accent,
                  textTransform: "uppercase", letterSpacing: "0.05em"
                }}>Coming Soon</div>
                <div style={{ fontSize: "0.9rem", fontWeight: "600", marginTop: "2px", color: "#aaa", fontStyle: "italic" }}>
                  {headline}
                </div>
              </div>
            ))}
          </Card>
        </div>

        {/* Row 2: Recent Results + AI Analysis */}
        {team.recent_games && team.recent_games.length > 0 && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem", marginBottom: "2rem" }}>
            <Card>
              <SectionHeading>Recent Form</SectionHeading>
              {/* W/D/L badge row */}
              <div style={{ display: "flex", gap: "6px", marginBottom: "1rem" }}>
                {team.recent_games.slice(0, 5).map((g, i) => {
                  const isHome = g.home_team_id === teamId;
                  const teamScore = isHome ? g.home_score : g.away_score;
                  const oppScore  = isHome ? g.away_score  : g.home_score;
                  const result = teamScore > oppScore ? "W" : teamScore < oppScore ? "L" : "D";
                  const opponent = isHome ? g.away_team_name : g.home_team_name;
                  return (
                    <div key={i}
                      title={`${result} vs ${opponent} (${g.home_score}–${g.away_score})`}
                      style={{
                        width: "34px", height: "34px", borderRadius: "50%",
                        background: RESULT_COLORS[result], color: "#fff",
                        fontWeight: "800", fontSize: "0.85rem",
                        display: "flex", alignItems: "center", justifyContent: "center"
                      }}
                    >
                      {result}
                    </div>
                  );
                })}
              </div>
              {/* Result rows */}
              {team.recent_games.map((g, i) => {
                const isHome = g.home_team_id === teamId;
                const teamScore = isHome ? g.home_score : g.away_score;
                const oppScore  = isHome ? g.away_score  : g.home_score;
                const result = teamScore > oppScore ? "W" : teamScore < oppScore ? "L" : "D";
                const opponent = isHome ? g.away_team_name : g.home_team_name;
                const oppId    = isHome ? g.away_team_id  : g.home_team_id;
                return (
                  <div key={i} style={{
                    display: "flex", justifyContent: "space-between", alignItems: "center",
                    padding: "6px 10px", borderRadius: "6px",
                    background: "#fafafa", border: "1px solid #f0f0f0",
                    marginBottom: "6px", fontSize: "0.88rem"
                  }}>
                    <span style={{ color: "#aaa", minWidth: "55px" }}>{g.date}</span>
                    <span style={{ flex: 1, padding: "0 0.75rem" }}>
                      {isHome ? "vs" : "@"}{" "}
                      <Link to={`/team/${oppId}`} style={{ color: "#222", textDecoration: "none" }}>
                        {opponent}
                      </Link>
                    </span>
                    <span style={{ fontWeight: "700", color: RESULT_COLORS[result], minWidth: "50px", textAlign: "right" }}>
                      {result} {g.home_score}–{g.away_score}
                    </span>
                  </div>
                );
              })}
            </Card>

            {/* AI Analysis placeholder */}
            <Card style={{ border: `2px dashed ${theme.accent}`, background: theme.light }}>
              <SectionHeading style={{ color: theme.accent }}>Contextual Analysis</SectionHeading>
              <p style={{ color: theme.accent, fontWeight: "600", marginBottom: "0.5rem", marginTop: 0 }}>
                AI-generated analysis coming soon
              </p>
              <p style={{ color: "#6b7280", fontSize: "0.9rem", lineHeight: "1.6", margin: 0 }}>
                Plain-English breakdowns of {team.name}'s recent form, what the numbers mean for the season,
                and how they compare to the rest of the league — written for every level of fan.
              </p>
            </Card>
          </div>
        )}

        {/* Roster */}
        {team.roster && team.roster.length > 0 && (
          <Card style={{ marginBottom: "2rem" }}>
            <SectionHeading>2026 Season Roster</SectionHeading>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
              <thead>
                <tr style={{ borderBottom: "2px solid #f0f0f0" }}>
                  {["Player", "Position", "Goals", "Assists", "xGoals", "Minutes"].map(h => (
                    <th key={h} style={{
                      textAlign: "left", padding: "8px 12px 8px 0",
                      fontSize: "0.8rem", color: "#888", fontWeight: "600"
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {team.roster.map(p => (
                  <tr key={p.player_id} style={{ borderBottom: "1px solid #f5f5f5" }}>
                    <td style={{ padding: "8px 12px 8px 0", fontWeight: "600" }}>
                      <Link
                        to={`/player/${p.player_name.toLowerCase().replace(/ /g, "-")}`}
                        style={{ color: theme.accent, textDecoration: "none" }}
                      >
                        {p.player_name}
                      </Link>
                    </td>
                    <td style={{ padding: "8px 12px 8px 0" }}>
                      {p.position ? (
                        <span style={{
                          display: "inline-block", padding: "1px 7px", borderRadius: "4px",
                          fontSize: "0.75rem", fontWeight: "700",
                          background: "#f3f4f6", color: "#555"
                        }}>
                          {p.position}
                        </span>
                      ) : "—"}
                    </td>
                    <td style={{ padding: "8px 12px 8px 0" }}>{p.goals ?? "—"}</td>
                    <td style={{ padding: "8px 12px 8px 0" }}>{p.assists ?? "—"}</td>
                    <td style={{ padding: "8px 12px 8px 0" }}>{p.xgoals ?? "—"}</td>
                    <td style={{ padding: "8px 12px 8px 0" }}>{p.minutes_played ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>
    </div>
  );
}
