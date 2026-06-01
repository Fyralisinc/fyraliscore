// Returns Authorization headers when a demo session token is present.
// All authed clients should call this so the demo flow can ride atop
// the existing endpoints without per-call refactors.

const TOKEN_KEY = "demoAuthToken";
const SESSION_KEY = "demoSessionId";

export function getDemoAuthToken(): string | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function getAuthHeader(): Record<string, string> {
  const token = getDemoAuthToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

// Manually set a bearer token — used by the GitHub Intelligence panel so a
// tester can authenticate against a specific tenant (the demo picker mints a
// fresh per-session tenant that can't see dogfood-seeded data).
export function setDemoAuthToken(token: string): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(TOKEN_KEY, token);
  } catch {
    // ignore
  }
}

// When the gateway rejects our token (server restarted, session pruned),
// drop the stale token and bounce the user to the picker so they can
// pick a company again instead of staring at endless 401s.
let _redirecting = false;
export function handleAuthFailure(): void {
  if (typeof window === "undefined") return;
  if (_redirecting) return;
  try {
    window.localStorage.removeItem(TOKEN_KEY);
    window.localStorage.removeItem(SESSION_KEY);
  } catch {
    // ignore
  }
  if (window.location.pathname === "/demo") return;
  _redirecting = true;
  window.location.replace("/demo");
}
