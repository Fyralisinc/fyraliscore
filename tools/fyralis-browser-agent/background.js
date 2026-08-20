const ALLOWED_ORIGINS = new Set([
  "http://localhost:8000",
  "http://127.0.0.1:8000"
]);

function endpointAllowed(endpoint) {
  try {
    const parsed = new URL(endpoint);
    return ALLOWED_ORIGINS.has(parsed.origin);
  } catch (_error) {
    return false;
  }
}

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || message.type !== "fyralis.slack.submit_config_token") {
    return false;
  }
  const endpoint = String(message.endpoint || "");
  const token = String(message.token || "");
  if (!endpointAllowed(endpoint)) {
    sendResponse({ ok: false, error: "endpoint_not_allowed" });
    return false;
  }
  if (!/^xoxe[-.][A-Za-z0-9._-]{8,}$/.test(token)) {
    sendResponse({ ok: false, error: "invalid_token_shape" });
    return false;
  }
  fetch(endpoint, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ inputs: { slack_app_config_token: token } })
  })
    .then(async (response) => {
      const text = await response.text();
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch (_error) {
        payload = { raw: text };
      }
      sendResponse({
        ok: response.ok,
        status: response.status,
        payload
      });
    })
    .catch((error) => {
      sendResponse({ ok: false, error: String(error && error.message || error) });
    });
  return true;
});
