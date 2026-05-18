import { useEffect, useState } from "react";

export default function MyNWSLPage() {
  const [teams, setTeams] = useState([]);
  const [favorites, setFavorites] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("http://localhost:8000/api/teams")
      .then(res => res.json())
      .then(data => setTeams(data.teams))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));

    fetch("http://localhost:8000/api/favorites")
      .then(res => res.json())
      .then(data => setFavorites(data.favorites || []));
  }, []);

  const saveFavorites = (updatedFavorites) => {
    fetch("http://localhost:8000/api/favorites", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ favorites: updatedFavorites }),
    });
  };

  const toggleFavorite = (teamId) => {
    const updatedFavorites = favorites.includes(teamId)
      ? favorites.filter(id => id !== teamId)
      : [...favorites, teamId];

    setFavorites(updatedFavorites);
    saveFavorites(updatedFavorites);
  };

  if (loading) return <p>Loading teams...</p>;
  if (error) return <p style={{ color: "red" }}>Error: {error}</p>;

  return (
    <div style={{ padding: "2rem", backgroundColor: "lightyellow" }}>
      <h1>My NWSL - Pick Your Favorite Teams</h1>
      {teams.map(team => (
        <div key={team.id} style={{ marginBottom: "1rem" }}>
          <button onClick={() => toggleFavorite(team.id)}>
            {favorites.includes(team.id) ? "★" : "☆"} {team.name}
          </button>
        </div>
      ))}

      <h2>Your Favorites</h2>
      {favorites.length === 0 ? (
        <p>No favorites selected.</p>
      ) : (
        <ul>
          {favorites.map(id => {
            const team = teams.find(t => t.id === id);
            return <li key={id}>{team?.name}</li>;
          })}
        </ul>
      )}
    </div>
  );
}
