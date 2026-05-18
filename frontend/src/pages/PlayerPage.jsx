import { useState } from "react";

export default function PlayerPage() {
  const player = {
    full_name: "Sophia Smith",
    position: "Forward",
    nationality: "USA",
    image_url: "https://example.com/sophia-smith.jpg",
    stats: {
      goals: 12,
      assists: 5,
      minutes_played: 980
    },
    merch: [
      {
        name: "Sophia Smith Home Jersey",
        image_url: "https://example.com/home-jersey.jpg",
        buy_link: "https://shop.example.com/home-jersey"
      },
      {
        name: "Sophia Smith Away Jersey",
        image_url: "https://example.com/away-jersey.jpg",
        buy_link: "https://shop.example.com/away-jersey"
      }
    ]
  };

  const [hypeStates, setHypeStates] = useState({
    cheer: false,
    highFive: false,
    lfg: false
  });

  const [hypeCounts, setHypeCounts] = useState({
    cheer: 0,
    highFive: 0,
    lfg: 0
  });

  const toggleHype = (type) => {
    setHypeStates((prevStates) => {
      const isCurrentlyActive = prevStates[type];
      setHypeCounts((prevCounts) => ({
        ...prevCounts,
        [type]: isCurrentlyActive ? prevCounts[type] - 1 : prevCounts[type] + 1
      }));
      return {
        ...prevStates,
        [type]: !isCurrentlyActive
      };
    });
  };

  return (
    <div style={{ padding: "2rem" }}>
      <h1>{player.full_name}</h1>
      <img src={player.image_url} alt={player.full_name} style={{ maxWidth: "100%", height: "auto" }} />
      <p>Position: {player.position}</p>
      <p>Nationality: {player.nationality}</p>

      <h2>Key Stats</h2>
      <ul>
        <li>Goals: {player.stats.goals}</li>
        <li>Assists: {player.stats.assists}</li>
        <li>Minutes Played: {player.stats.minutes_played}</li>
      </ul>

      <h2>Shop Her Look</h2>
      {player.merch.map((item, idx) => (
        <div key={idx} style={{ border: "1px solid #ccc", padding: "1rem", marginBottom: "1rem" }}>
          <img src={item.image_url} alt={item.name} style={{ maxWidth: "100px", height: "auto" }} />
          <p>{item.name}</p>
          <a href={item.buy_link} target="_blank" rel="noopener noreferrer">🛍️ Shop Now</a>
        </div>
      ))}

      <h2>Hype Her Up!</h2>
      <button
        onClick={() => toggleHype("cheer")}
        style={{ backgroundColor: hypeStates.cheer ? "yellow" : "white" }}
      >
        🎉 Cheer ({hypeCounts.cheer})
      </button>

      <button
        onClick={() => toggleHype("highFive")}
        style={{ backgroundColor: hypeStates.highFive ? "yellow" : "white" }}
      >
        🙌 High Five ({hypeCounts.highFive})
      </button>

      <button
        onClick={() => toggleHype("lfg")}
        style={{ backgroundColor: hypeStates.lfg ? "yellow" : "white" }}
      >
        🔥 LFG ({hypeCounts.lfg})
      </button>
    </div>
  );
}
