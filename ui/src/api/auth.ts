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
