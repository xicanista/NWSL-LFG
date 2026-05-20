import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";

const RESULT_COLORS = { W: "#16a34a", D: "#ca8a04", L: "#dc2626" };

export default function PlayerPage() {
  const { slug } = useParams();
  const [player, setPlayer] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [hypeStates, setHypeStates] = useState({ cheer: false, highFive: false, lfg: false });
  const [hypeCounts, setHypeCounts] = useState({ cheer: 0, highFive: 0, lfg: 0 });

  useEffect(() => {
    fetch(`http://localhost:8000/api/player/${slug}`)
      .then(res => res.json())
      .then(data => {
        if (data.error) setError(data.error);
        else setPlayer(data);
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [slug]);

  const toggleHype = (type) => {
    setHypeStates(prev => {
      const active = prev[type];
      setHypeCounts(c => ({ ...c, [type]: active ? c[type] - 1 : c[type] + 1 }));
      return { ...prev, [type]: !active };
    });
  };

  if (loading) return <p style={{ padding: "2rem" }}>Loading player...</p>;
  if (error) return <p style={{ padding: "2rem", color: "red" }}>Error: {error}</p>;

  const s = player.stats;

  return (
    <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem" }}>
      <Link to="/" style={{ color: "#7c3aed", textDecoration: "none" }}>← Back</Link>

      <h1 style={{ marginTop: "1rem" }}>{player.full_name}</h1>

      {/* Bio */}
      <div style={{ display: "flex", gap: "2rem", flexWrap: "wrap", color: "#555", marginBottom: "1.5rem" }}>
        {player.nationality && <span>🌍 {player.nationality}</span>}
        {player.position && <span>📍 {player.position}</span>}
        {player.height && <span>📏 {player.height}</span>}
        {player.birth_date && <span>🎂 {new Date(player.birth_date).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })}</span>}
        {s?.team && <span>🏟️ {s.team}</span>}
      </div>

      {/* Hype buttons */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "2rem" }}>
        {[
          { key: "cheer", emoji: "🎉", label: "Cheer" },
          { key: "highFive", emoji: "🙌", label: "High Five" },
          { key: "lfg", emoji: "🔥", label: "LFG" },
        ].map(({ key, emoji, label }) => (
          <button
            key={key}
            onClick={() => toggleHype(key)}
            style={{
              padding: "0.4rem 1rem", borderRadius: "20px", border: "1px solid #e0e0e0",
              backgroundColor: hypeStates[key] ? "#fef9c3" : "#fff",
              cursor: "pointer", fontWeight: "500"
            }}
          >
            {emoji} {label} ({hypeCounts[key]})
          </button>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem" }}>

        {/* Season Stats */}
        {s && Object.keys(s).length > 0 && (
          <section>
            <h2>Stats {s.season ? `— ${s.season} Season` : ""}</h2>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <tbody>
                {[
                  ["Goals", s.goals, "Total goals scored this season."],
                  ["Assists", s.assists, "Goals directly set up for a teammate."],
                  ["Shots", s.shots, "Total shots attempted — reflects attacking involvement."],
                  ["Shots on Target", s.shots_on_target, "Shots that forced the goalkeeper to make a save or resulted in a goal."],
                  ["xGoals (xG)", s.xgoals, "Expected goals — how many goals she *should* have scored based on shot quality. Higher than actual = unlucky; lower = clinical."],
                  ["Minutes Played", s.minutes_played, "Total minutes on the pitch this season."],
                ].filter(([, v]) => v != null).map(([label, value, explain]) => (
                  <StatRow key={label} label={label} value={value} explain={explain} />
                ))}
              </tbody>
            </table>
          </section>
        )}

        {/* Recent Form */}
        {player.recent_games && player.recent_games.length > 0 && (
          <section>
            <h2>Recent Form</h2>
            <div style={{ display: "flex", gap: "6px", marginBottom: "1rem" }}>
              {player.recent_games.map((g, i) => (
                <div key={i} title={`${g.result} vs ${g.opponent} (${g.score})`} style={{
                  width: "32px", height: "32px", borderRadius: "50%",
                  backgroundColor: RESULT_COLORS[g.result],
                  color: "#fff", fontWeight: "bold", fontSize: "0.85rem",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  cursor: "default"
                }}>
                  {g.result}
                </div>
              ))}
            </div>
            <div style={{ display: "grid", gap: "0.5rem" }}>
              {player.recent_games.map((g, i) => (
                <div key={i} style={{
                  display: "flex", justifyContent: "space-between",
                  padding: "0.5rem 0.75rem", borderRadius: "6px",
                  backgroundColor: "#fafafa", border: "1px solid #e0e0e0",
                  fontSize: "0.9rem"
                }}>
                  <span style={{ color: "#888" }}>{g.date}</span>
                  <span>{g.home ? "vs" : "@"} {g.opponent}</span>
                  <span style={{ fontWeight: "bold", color: RESULT_COLORS[g.result] }}>
                    {g.result} {g.score}
                  </span>
                </div>
              ))}
            </div>
          </section>
        )}
      </div>

      {/* Analysis placeholder */}
      <section style={{ marginTop: "2rem" }}>
        <h2>Contextual Analysis</h2>
        <div style={{
          padding: "1.25rem", border: "2px dashed #c4b5fd",
          borderRadius: "8px", backgroundColor: "#faf5ff"
        }}>
          <p style={{ margin: 0, color: "#7c3aed", fontWeight: "600" }}>AI-generated analysis coming soon</p>
          <p style={{ margin: "0.5rem 0 0", color: "#6b7280", fontSize: "0.9rem" }}>
            Plain-English breakdowns of {player.full_name}'s recent form, what her numbers mean for her team, and how she compares to the league — written for every level of fan.
          </p>
        </div>
      </section>
    </div>
  );
}

function StatRow({ label, value, explain }) {
  const [open, setOpen] = useState(false);
  return (
    <tr style={{ borderBottom: "1px solid #eee" }}>
      <td style={{ padding: "8px 12px 8px 0", fontWeight: "600", verticalAlign: "top" }}>
        {label}
        <button
          onClick={() => setOpen(!open)}
          style={{
            marginLeft: "6px", background: "none", border: "1px solid #c4b5fd",
            borderRadius: "50%", width: "18px", height: "18px", cursor: "pointer",
            fontSize: "11px", color: "#7c3aed", lineHeight: "16px", padding: 0
          }}
        >?</button>
      </td>
      <td style={{ padding: "8px 0", verticalAlign: "top" }}>
        <div>{value}</div>
        {open && (
          <div style={{
            marginTop: "4px", fontSize: "0.85rem", color: "#555",
            backgroundColor: "#faf5ff", padding: "6px 10px",
            borderRadius: "6px", borderLeft: "3px solid #7c3aed"
          }}>
            {explain}
          </div>
        )}
      </td>
    </tr>
  );
}
