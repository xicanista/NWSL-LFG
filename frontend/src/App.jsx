import { HashRouter as Router, Routes, Route, Link } from "react-router-dom";
import PlayerPage from "./pages/PlayerPage";
import MyNWSLPage from "./pages/MyNWSLPage";

function HomePage() {
  return (
    <div style={{ padding: "2rem" }}>
      <h1>Home Page</h1>
      <Link to="/player/sophia-smith">Go to Sophia Smith's Page</Link><br />
      <Link to="/my-nwsl">Go to My NWSL Page</Link>
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
        <Route path="player/:firstname-:lastname" element={<PlayerPage />} />
        <Route path="my-nwsl" element={<MyNWSLPage />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </Router>
  );
}

export default App;
