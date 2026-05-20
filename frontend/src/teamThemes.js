// Official NWSL club color palettes (2025)
// primary: hero/header background
// primaryText: text on primary background
// accent: buttons, links, highlights
// accentText: text on accent color
// light: soft tinted background for cards/rows

const themes = {
  // Angel City FC
  kRQa8JOqKZ: {
    primary: "#202121",
    primaryText: "#F1B1A5",
    accent: "#F1B1A5",
    accentText: "#202121",
    light: "#fdf5f3",
  },
  // Bay FC
  "315VnJ759x": {
    primary: "#0D2032",
    primaryText: "#ffffff",
    accent: "#FF5049",
    accentText: "#ffffff",
    light: "#f5f8fa",
  },
  // Boston Breakers (defunct)
  "4wM4Ezg5jB": {
    primary: "#1a1a2e",
    primaryText: "#ffffff",
    accent: "#e94560",
    accentText: "#ffffff",
    light: "#f5f5f8",
  },
  // Chicago Stars FC
  KPqjw8PQ6v: {
    primary: "#102B45",
    primaryText: "#ffffff",
    accent: "#3AB5E8",
    accentText: "#102B45",
    light: "#f0f8fd",
  },
  // Denver Summit FC (not in 2025 PDF — mountain-inspired)
  "2lqRn34qr0": {
    primary: "#1B3A4B",
    primaryText: "#ffffff",
    accent: "#4A9B8E",
    accentText: "#ffffff",
    light: "#f0f7f6",
  },
  // Houston Dash
  "4JMAk47qKg": {
    primary: "#101820",
    primaryText: "#ffffff",
    accent: "#FF6900",
    accentText: "#ffffff",
    light: "#fff5ee",
  },
  // Kansas City Current
  "4wM4rZdqjB": {
    primary: "#081F2C",
    primaryText: "#ffffff",
    accent: "#62CBC9",
    accentText: "#081F2C",
    light: "#f0fafa",
  },
  // NJ/NY Gotham FC
  raMyrr25d2: {
    primary: "#000000",
    primaryText: "#A9F1FD",
    accent: "#A9F1FD",
    accentText: "#000000",
    light: "#f0fdff",
  },
  // North Carolina Courage
  zeQZeazqKw: {
    primary: "#AB0033",
    primaryText: "#ffffff",
    accent: "#B4A269",
    accentText: "#ffffff",
    light: "#fdf5f7",
  },
  // Orlando Pride
  XVqKeVKM01: {
    primary: "#391A66",
    primaryText: "#ffffff",
    accent: "#00ABFF",
    accentText: "#ffffff",
    light: "#f5f0fc",
  },
  // Portland Thorns FC
  Pk5LeeNqOW: {
    primary: "#000000",
    primaryText: "#ffffff",
    accent: "#99242B",
    accentText: "#ffffff",
    light: "#fff5f5",
  },
  // Racing Louisville FC
  eV5DR6YQKn: {
    primary: "#14002F",
    primaryText: "#C5B5F2",
    accent: "#C5B5F2",
    accentText: "#14002F",
    light: "#f7f4ff",
  },
  // San Diego Wave FC
  "7VqG1lYMvW": {
    primary: "#032E62",
    primaryText: "#ffffff",
    accent: "#21C6D9",
    accentText: "#032E62",
    light: "#f0fbfc",
  },
  // Seattle Reign FC
  "7vQ7BBzqD1": {
    primary: "#292431",
    primaryText: "#ffffff",
    accent: "#D0A66B",
    accentText: "#292431",
    light: "#faf8f4",
  },
  // Utah Royals FC
  eV5D2w9QKn: {
    primary: "#0E1735",
    primaryText: "#ffffff",
    accent: "#FDB71A",
    accentText: "#0E1735",
    light: "#fffbf0",
  },
  // Washington Spirit
  aDQ0lzvQEv: {
    primary: "#000000",
    primaryText: "#EDE939",
    accent: "#EDE939",
    accentText: "#000000",
    light: "#fefef0",
  },
  // Western New York Flash (defunct)
  xW5pwDBMg1: {
    primary: "#1a1a2e",
    primaryText: "#ffffff",
    accent: "#4a4a8a",
    accentText: "#ffffff",
    light: "#f5f5f8",
  },
};

const defaultTheme = {
  primary: "#111827",
  primaryText: "#ffffff",
  accent: "#7c3aed",
  accentText: "#ffffff",
  light: "#f9f5ff",
};

export function getTeamTheme(teamId) {
  return themes[teamId] || defaultTheme;
}
