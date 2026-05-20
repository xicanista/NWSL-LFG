import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";

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
    setHypeStates((prevStates) => {
      const isCurrentlyActive = prevStates[type];
      setHypeCounts((prevCounts) => ({
        ...prevCounts,
        [type]: isCurrentlyActive ? prevCounts[type] - 1 : prevCounts[type] + 1
      }));
      return { ...prevStates, [type]: !isCurrentlyActive };
    });
  };

  if (loading) return <p style={{ padding: "2rem" }}>Loading player...</p>;
  if (error) return <p style={{ padding: "2rem", color: "red" }}>Error: {error}</p>;

  return (
    <div style={{ padding: "2rem" }}>
      <h1>{player.full_name}</h1>
      <p>Position: {player.position || "—"}</p>
      <p>Nationality: {player.nationality || "—"}</p>

      {player.stats && Object.keys(player.stats).length > 0 && (
        <>
          <h2>Stats {player.stats.season ? `(${player.stats.season} Season)` : ""}</h2>
          {player.stats.team && <p>Team: {player.stats.team}</p>}
          <ul>
            {player.stats.goals != null && <li>Goals: {player.stats.goals}</li>}
            {player.stats.assists != null && <li>Assists: {player.stats.assists}</li>}
            {player.stats.shots != null && <li>Shots: {player.stats.shots}</li>}
            {player.stats.xgoals != null && <li>xGoals: {player.stats.xgoals}</li>}
            {player.stats.minutes_played != null && <li>Minutes Played: {player.stats.minutes_played}</li>}
          </ul>
        </>
      )}

      {player.merch && player.merch.length > 0 && (
        <>
          <h2>Shop Her Look</h2>
          {player.merch.map((item, idx) => (
            <div key={idx} style={{ border: "1px solid #ccc", padding: "1rem", marginBottom: "1rem" }}>
              <p>{item.name}</p>
              <a href={item.buy_link} target="_blank" rel="noopener noreferrer">🛍️ Shop Now</a>
            </div>
          ))}
        </>
      )}

      <h2>Hype Her Up!</h2>
      <button onClick={() => toggleHype("cheer")} style={{ backgroundColor: hypeStates.cheer ? "yellow" : "white", marginRight: "0.5rem" }}>
        🎉 Cheer ({hypeCounts.cheer})
      </button>
      <button onClick={() => toggleHype("highFive")} style={{ backgroundColor: hypeStates.highFive ? "yellow" : "white", marginRight: "0.5rem" }}>
        🙌 High Five ({hypeCounts.highFive})
      </button>
      <button onClick={() => toggleHype("lfg")} style={{ backgroundColor: hypeStates.lfg ? "yellow" : "white" }}>
        🔥 LFG ({hypeCounts.lfg})
      </button>
    </div>
  );
}
