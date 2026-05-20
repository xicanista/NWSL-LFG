import { HashRouter as Router, Routes, Route, Link } from "react-router-dom";
import { useEffect, useState } from "react";
import PlayerPage from "./pages/PlayerPage";
import MyNWSLPage from "./pages/MyNWSLPage";
import TeamPage from "./pages/TeamPage";

function HomePage() {
  const [teams, setTeams] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8000/api/teams")
      .then(res => res.json())
      .then(data => setTeams(data.teams || []));
  }, []);

  return (
    <div style={{ padding: "2rem" }}>
      <h1>Home Page</h1>
      <Link to="/my-nwsl">Go to My NWSL Page</Link><br /><br />

      <h2>Teams</h2>
      <ul>
        {teams.map(team => (
          <li key={team.id}>
            <Link to={`/team/${team.id}`}>{team.name}</Link>
          </li>
        ))}
      </ul>

      <h2>Players</h2>
      <Link to="/player/sophia-wilson">Sophia Wilson</Link><br />
      <Link to="/player/alex-morgan">Alex Morgan</Link><br />
      <Link to="/player/debinha">Debinha</Link><br />
      <Link to="/player/sam-kerr">Sam Kerr</Link><br />
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
