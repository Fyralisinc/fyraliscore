/** @type {import('tailwindcss').Config} */
// Mirrors the CSS variables declared in /company-os.html lines 11-44.
// Every color, font, and radius we use in the CEO view should resolve
// through this config rather than magic hex strings in components.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0a0b0d",
        "bg-elevated": "#0f1114",
        surface: "#14171b",
        "surface-2": "#1a1e23",
        "surface-3": "#21262c",
        "surface-4": "#2a3038",
        border: "#242a31",
        "border-2": "#323942",
        "border-3": "#404853",
        ink: "#eaecef",
        "ink-2": "#b8bdc4",
        "ink-3": "#7a8089",
        "ink-4": "#4f555d",
        "ink-5": "#353a40",
        hot: "#d85a42",
        "hot-2": "#e47258",
        "hot-dim": "rgba(216, 90, 66, 0.14)",
        "hot-faint": "rgba(216, 90, 66, 0.07)",
        warm: "#c8973a",
        cool: "#6eb08a",
        soft: "#7a90b8",
      },
      fontFamily: {
        sans: [
          "Inter",
          "-apple-system",
          "BlinkMacSystemFont",
          "Segoe UI",
          "sans-serif",
        ],
        serif: ["Fraunces", "Georgia", "serif"],
        mono: ["JetBrains Mono", "SF Mono", "Menlo", "monospace"],
      },
      borderRadius: {
        sm: "3px",
        DEFAULT: "4px",
        md: "6px",
      },
      transitionTimingFunction: {
        arrive: "cubic-bezier(0.2, 0.7, 0.2, 1)",
      },
      keyframes: {
        arrive: {
          from: { opacity: "0", transform: "translateY(4px)" },
          to: { opacity: "1", transform: "translateY(0)" },
        },
        pulse3: {
          "0%,100%": { opacity: "1" },
          "50%": { opacity: "0.5" },
        },
      },
      animation: {
        arrive: "arrive 360ms cubic-bezier(0.2, 0.7, 0.2, 1) backwards",
        pulse3: "pulse3 3s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
