import { HashRouter as Router, Routes, Route, Link } from "react-router-dom";
import { useEffect, useState } from "react";
import PlayerPage from "./pages/PlayerPage";
import MyNWSLPage from "./pages/MyNWSLPage";
import TeamPage from "./pages/TeamPage";
import { getTeamTheme } from "./teamThemes";

function HomePage() {
  const [teams, setTeams] = useState([]);
  const [matches, setMatches] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8000/api/teams")
      .then(res => res.json())
      .then(data => setTeams(data.teams || []));

    fetch("http://localhost:8000/api/recent-matches")
      .then(res => res.json())
      .then(data => setMatches(data.matches || []));
  }, []);

  return (
    <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem" }}>
      <h1>⚽ NWSL-LFG</h1>
      <p style={{ color: "#555", marginBottom: "2rem" }}>
        Built for female NWSL fans. Real stats, plain English, and a community to watch with.
      </p>

      {/* Latest Results */}
      <section style={{ marginBottom: "3rem" }}>
        <h2>Latest Results</h2>
        <div style={{ display: "grid", gap: "0.75rem" }}>
          {matches.map(m => (
            <div key={m.game_id} style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              padding: "0.75rem 1rem", border: "1px solid #e0e0e0", borderRadius: "8px",
              backgroundColor: "#fafafa"
            }}>
              <div style={{ flex: 1, textAlign: "right" }}>
                <Link to={`/team/${m.home_team_id}`} style={{ fontWeight: "bold", textDecoration: "none", color: "#222" }}>
                  {m.home_team_name}
                </Link>
              </div>
              <div style={{ padding: "0 1rem", fontWeight: "bold", fontSize: "1.1rem", minWidth: "80px", textAlign: "center" }}>
                {m.home_score} – {m.away_score}
              </div>
              <div style={{ flex: 1 }}>
                <Link to={`/team/${m.away_team_id}`} style={{ fontWeight: "bold", textDecoration: "none", color: "#222" }}>
                  {m.away_team_name}
                </Link>
              </div>
              <div style={{ color: "#888", fontSize: "0.85rem", marginLeft: "1rem", minWidth: "90px", textAlign: "right" }}>
                {m.date}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Analysis Previews (stubbed) */}
      <section style={{ marginBottom: "3rem" }}>
        <h2>Latest Analysis</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: "1rem" }}>
          {[
            { title: "xG Explained: Why Your Team Deserved That Win", tag: "Stat Explainer" },
            { title: "Week 12 Breakdown: What the Numbers Say", tag: "Match Analysis" },
            { title: "Rising Stars: The Players Outperforming Their xG", tag: "Feature" },
          ].map((article, i) => (
            <div key={i} style={{
              padding: "1.25rem", border: "1px solid #e0e0e0", borderRadius: "8px",
              backgroundColor: "#fff", cursor: "pointer"
            }}>
              <div style={{
                display: "inline-block", fontSize: "0.75rem", fontWeight: "bold",
                color: "#7c3aed", backgroundColor: "#ede9fe",
                padding: "2px 8px", borderRadius: "4px", marginBottom: "0.5rem"
              }}>
                {article.tag}
              </div>
              <p style={{ margin: 0, fontWeight: "600", color: "#222" }}>{article.title}</p>
              <p style={{ margin: "0.5rem 0 0", fontSize: "0.85rem", color: "#888" }}>
                Coming soon — full analysis dropping this week.
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* Watch-Along Preview (placeholder) */}
      <section style={{ marginBottom: "3rem" }}>
        <h2>Watch-Along Rooms 💬</h2>
        <div style={{
          padding: "1.5rem", border: "2px dashed #c4b5fd", borderRadius: "8px",
          backgroundColor: "#faf5ff", textAlign: "center"
        }}>
          <p style={{ fontSize: "1.1rem", fontWeight: "600", color: "#7c3aed", margin: "0 0 0.5rem" }}>
            Live rooms open when matches kick off
          </p>
          <p style={{ color: "#6b7280", margin: "0 0 1rem" }}>
            Join thousands of fans watching together in real time — no Twitter chaos, just good vibes and great soccer.
          </p>
          <div style={{ display: "flex", gap: "0.75rem", justifyContent: "center", flexWrap: "wrap" }}>
            {["Portland Thorns FC", "Angel City FC", "NJ/NY Gotham FC"].map(team => (
              <div key={team} style={{
                padding: "0.5rem 1rem", backgroundColor: "#ede9fe",
                borderRadius: "20px", fontSize: "0.9rem", color: "#7c3aed", fontWeight: "500"
              }}>
                🔴 {team} — Room Opening Soon
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Teams */}
      <section style={{ marginBottom: "3rem" }}>
        <h2>Teams</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: "0.5rem" }}>
          {teams.map(team => {
            const theme = getTeamTheme(team.id);
            return (
              <Link key={team.id} to={`/team/${team.id}`} style={{
                padding: "0.75rem 1rem", borderRadius: "8px",
                textDecoration: "none", fontWeight: "600", fontSize: "0.95rem",
                backgroundColor: theme.primary, color: theme.primaryText,
                display: "block", transition: "opacity 0.15s",
              }}
              onMouseEnter={e => e.currentTarget.style.opacity = "0.85"}
              onMouseLeave={e => e.currentTarget.style.opacity = "1"}
              >
                {team.name}
              </Link>
            );
          })}
        </div>
      </section>

      {/* Featured Players */}
      <section>
        <h2>Featured Players</h2>
        <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
          {[
            { name: "Sophia Wilson", slug: "sophia-wilson" },
            { name: "Alex Morgan", slug: "alex-morgan" },
            { name: "Debinha", slug: "debinha" },
            { name: "Sam Kerr", slug: "sam-kerr" },
          ].map(p => (
            <Link key={p.slug} to={`/player/${p.slug}`} style={{
              padding: "0.6rem 1.2rem", border: "1px solid #e0e0e0", borderRadius: "6px",
              textDecoration: "none", color: "#222", backgroundColor: "#fafafa", fontWeight: "500"
            }}>
              {p.name}
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}

function NotFound() {
  return (
    <div style={{ padding: "2rem", color: "red" }}>
      <h1>404 - Page Not Found</h1>
    </div>
  );
}

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="player/:slug" element={<PlayerPage />} />
        <Route path="my-nwsl" element={<MyNWSLPage />} />
        <Route path="team/:teamId" element={<TeamPage />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </Router>
  );
}

export default App;
