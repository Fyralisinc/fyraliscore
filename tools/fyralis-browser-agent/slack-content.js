(function () {
  const TOKEN_RE = /\bxoxe[-.][A-Za-z0-9._-]{8,}\b/;
  const HANDOFF_PREFIX = "fyralis_agent=";
  const HANDOFF_STORAGE_KEY = "fyralis.slack.handoff.v1";
  const DEFAULT_LOCAL_ENDPOINT =
    "http://localhost:8000/platform/onboarding/sources/slack/rehearsal/browser-agent/configuration";
  const POLL_MS = 750;
  const MAX_ATTEMPTS = 240;
  let submittedToken = null;
  let runInProgress = false;
  let accessTokenCopyClicked = false;
  let fallbackShown = false;

  function decodeHandoff() {
    const hash = window.location.hash.replace(/^#/, "");
    const part = hash.split("&").find((item) => item.startsWith(HANDOFF_PREFIX));
    if (!part) {
      return storedHandoff();
    }
    try {
      const encoded = part.slice(HANDOFF_PREFIX.length);
      const base64 = decodeURIComponent(encoded).replace(/-/g, "+").replace(/_/g, "/");
      const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
      const payload = JSON.parse(atob(padded));
      storeHandoff(payload);
      return payload;
    } catch (_error) {
      return storedHandoff();
    }
  }

  function storeHandoff(payload) {
    try {
      window.localStorage.setItem(
        HANDOFF_STORAGE_KEY,
        JSON.stringify({
          payload,
          storedAt: Date.now()
        })
      );
    } catch (_error) {
      // localStorage can be disabled by browser policy; the fragment path still works.
    }
  }

  function storedHandoff() {
    try {
      const raw = window.localStorage.getItem(HANDOFF_STORAGE_KEY);
      if (!raw) {
        return null;
      }
      const record = JSON.parse(raw);
      if (!record || Date.now() - Number(record.storedAt || 0) > 10 * 60 * 1000) {
        window.localStorage.removeItem(HANDOFF_STORAGE_KEY);
        return null;
      }
      return record.payload || null;
    } catch (_error) {
      return null;
    }
  }

  function statusPanel() {
    let panel = document.getElementById("fyralis-browser-agent-status");
    if (panel) {
      return panel;
    }
    panel = document.createElement("div");
    panel.id = "fyralis-browser-agent-status";
    panel.style.cssText = [
      "position:fixed",
      "right:20px",
      "bottom:20px",
      "z-index:2147483647",
      "max-width:360px",
      "padding:14px 16px",
      "border-radius:8px",
      "background:#0b1020",
      "color:#f8fafc",
      "font:14px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif",
      "box-shadow:0 16px 40px rgba(15,23,42,.28)"
    ].join(";");
    document.documentElement.appendChild(panel);
    return panel;
  }

  function setStatus(message) {
    const panel = statusPanel();
    panel.textContent = "";
    const text = document.createElement("div");
    text.textContent = message;
    panel.appendChild(text);
  }

  function showPasteFallback(handoff, message) {
    fallbackShown = true;
    const panel = statusPanel();
    panel.textContent = "";
    const text = document.createElement("div");
    text.textContent = message;
    panel.appendChild(text);
    const readClipboard = document.createElement("button");
    readClipboard.type = "button";
    readClipboard.textContent = "Read copied token";
    readClipboard.style.cssText = [
      "margin-top:10px",
      "width:100%",
      "padding:8px",
      "border:0",
      "border-radius:6px",
      "background:#38bdf8",
      "color:#020617",
      "font-weight:700",
      "cursor:pointer"
    ].join(";");
    readClipboard.addEventListener("click", async () => {
      const token = await tokenFromClipboard();
      if (token) {
        await submitDetectedToken(handoff, token);
      } else {
        setStatus("Clipboard does not contain a Slack token yet. Click Slack's Access Token Copy button, then try again.");
        showPasteFallback(handoff, "Or paste the generated Slack token here.");
      }
    });
    panel.appendChild(readClipboard);
    const input = document.createElement("input");
    input.type = "password";
    input.placeholder = "xoxe...";
    input.autocomplete = "off";
    input.style.cssText = [
      "margin-top:10px",
      "width:100%",
      "box-sizing:border-box",
      "padding:8px",
      "border:1px solid #475569",
      "border-radius:6px",
      "background:#020617",
      "color:#f8fafc"
    ].join(";");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Submit pasted token";
    button.style.cssText = [
      "margin-top:8px",
      "width:100%",
      "padding:8px",
      "border:0",
      "border-radius:6px",
      "background:#38bdf8",
      "color:#020617",
      "font-weight:700",
      "cursor:pointer"
    ].join(";");
    button.addEventListener("click", async () => {
      const token = input.value.trim();
      if (!TOKEN_RE.test(token)) {
        setStatus("That does not look like a Slack app configuration token.");
        showPasteFallback(handoff, "Paste the generated Slack token here.");
        return;
      }
      await submitDetectedToken(handoff, token);
    });
    panel.appendChild(input);
    panel.appendChild(button);
  }

  function visibleText(element) {
    return String(element && (element.innerText || element.textContent) || "").trim();
  }

  function buttonText(element) {
    return String(
      visibleText(element) ||
        element.getAttribute("aria-label") ||
        element.getAttribute("title") ||
        element.value ||
        ""
    ).trim();
  }

  function isVisible(element) {
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return (
      style.visibility !== "hidden" &&
      style.display !== "none" &&
      rect.width > 0 &&
      rect.height > 0
    );
  }

  function actionableElements(root) {
    return Array.from(
      root.querySelectorAll("button, a, input[type='button'], input[type='submit']")
    ).filter(isVisible);
  }

  function clickButtonMatching(root, patterns, options) {
    const blockedText = options && options.blockedText;
    const candidates = Array.from(
      actionableElements(root)
    );
    for (const element of candidates) {
      const text = buttonText(element);
      const surroundingText = visibleText(element.closest("[role='dialog'], section, div") || element);
      if (
        patterns.some((pattern) => pattern.test(text)) &&
        !(blockedText && blockedText.test(`${text}\n${surroundingText}`))
      ) {
        element.click();
        return true;
      }
    }
    return false;
  }

  function closestSection(element) {
    let current = element;
    for (let depth = 0; current && depth < 6; depth += 1) {
      const text = visibleText(current);
      if (/your app configuration tokens|app configuration tokens/i.test(text)) {
        return current;
      }
      current = current.parentElement;
    }
    return null;
  }

  function tokenSection() {
    const candidates = Array.from(document.querySelectorAll("h1, h2, h3, h4, div, section"));
    for (const element of candidates) {
      if (/your app configuration tokens|app configuration tokens/i.test(visibleText(element))) {
        const section = closestSection(element);
        if (section) {
          return section;
        }
      }
    }
    return document;
  }

  function wrongCreateAppDialog() {
    const dialogs = Array.from(document.querySelectorAll("[role='dialog'], [aria-modal='true']"));
    return dialogs.find((dialog) => {
      const text = visibleText(dialog);
      return /create an app/i.test(text) && /from a manifest/i.test(text) && /from scratch/i.test(text);
    }) || null;
  }

  function closeWrongCreateAppDialog() {
    const dialog = wrongCreateAppDialog();
    if (!dialog) {
      return false;
    }
    const closed = clickButtonMatching(
      dialog,
      [/^close$/i, /^×$/i, /^x$/i]
    );
    if (!closed) {
      document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    }
    setStatus("Fyralis closed Slack's app-creation dialog. Retrying token generation...");
    return true;
  }

  function clickGenerateToken() {
    const root = tokenSection();
    return clickButtonMatching(
      root,
      [/^generate token$/i, /^create token$/i],
      { blockedText: /create new app|create an app|from a manifest|from scratch/i }
    );
  }

  function clickTokenDialogAction() {
    const dialogs = Array.from(document.querySelectorAll("[role='dialog'], [aria-modal='true']"));
    const tokenDialogs = dialogs.filter((dialog) => /token|configuration/i.test(visibleText(dialog)));
    for (const dialog of tokenDialogs) {
      if (
        clickButtonMatching(
          dialog,
          [/^generate token$/i, /^create token$/i, /^generate$/i, /^allow$/i, /^approve$/i, /^continue$/i, /^confirm$/i],
          { blockedText: /create an app|from a manifest|from scratch/i }
        )
      ) {
        return true;
      }
    }
    return false;
  }

  function tokenFromString(value) {
    const match = String(value || "").match(TOKEN_RE);
    return match ? match[0] : null;
  }

  function tokenFromDom() {
    const fields = Array.from(document.querySelectorAll("input, textarea, code, pre"));
    for (const field of fields) {
      const value = String(
        field.value ||
          field.getAttribute("value") ||
          field.textContent ||
          field.getAttribute("aria-label") ||
          field.getAttribute("title") ||
          ""
      );
      const token = tokenFromString(value);
      if (token) {
        return token;
      }
    }
    const attributeToken = tokenFromAttributes();
    if (attributeToken) {
      return attributeToken;
    }
    const bodyToken = tokenFromString(document.body && document.body.innerText);
    if (bodyToken) {
      return bodyToken;
    }
    return tokenFromString(document.documentElement && document.documentElement.innerHTML);
  }

  function tokenFromAttributes() {
    const elements = Array.from(document.querySelectorAll("*"));
    for (const element of elements) {
      for (const attribute of Array.from(element.attributes || [])) {
        const token = tokenFromString(attribute.value);
        if (token) {
          return token;
        }
      }
    }
    return null;
  }

  async function tokenFromClipboard() {
    try {
      if (!navigator.clipboard || !navigator.clipboard.readText) {
        return null;
      }
      return tokenFromString(await navigator.clipboard.readText());
    } catch (_error) {
      return null;
    }
  }

  function accessTokenCopyButton() {
    const root = tokenSection();
    const buttons = actionableElements(root).filter((button) => /^copy$/i.test(buttonText(button)));
    if (!buttons.length) {
      return null;
    }
    const accessHeader = Array.from(root.querySelectorAll("*")).find((element) =>
      /^access token$/i.test(visibleText(element))
    );
    if (!accessHeader) {
      return buttons[0];
    }
    const headerRect = accessHeader.getBoundingClientRect();
    let best = null;
    let bestScore = Number.POSITIVE_INFINITY;
    for (const button of buttons) {
      const rect = button.getBoundingClientRect();
      const verticalDistance = Math.max(0, rect.top - headerRect.bottom);
      const horizontalDistance = Math.abs(rect.left - headerRect.left);
      const score = verticalDistance * 10 + horizontalDistance;
      if (rect.top >= headerRect.top && score < bestScore) {
        best = button;
        bestScore = score;
      }
    }
    return best || buttons[0];
  }

  async function tokenFromAccessTokenCopyButton() {
    const button = accessTokenCopyButton();
    if (!button) {
      return null;
    }
    const attributeToken = tokenFromString(
      [
        button.getAttribute("data-clipboard-text"),
        button.getAttribute("data-copy-text"),
        button.getAttribute("data-token"),
        button.getAttribute("aria-label"),
        button.getAttribute("title")
      ].join(" ")
    );
    if (attributeToken) {
      return attributeToken;
    }
    if (!accessTokenCopyClicked) {
      accessTokenCopyClicked = true;
      button.click();
      setStatus("Fyralis clicked Access Token Copy. Reading clipboard...");
      await new Promise((resolve) => setTimeout(resolve, 350));
    }
    return await tokenFromClipboard();
  }

  function submitToken(endpoint, token) {
    return new Promise((resolve) => {
      chrome.runtime.sendMessage(
        {
          type: "fyralis.slack.submit_config_token",
          endpoint,
          token
        },
        (response) => resolve(response || { ok: false, error: "no_extension_response" })
      );
    });
  }

  async function submitDetectedToken(handoff, token) {
    if (submittedToken === token) {
      return true;
    }
    submittedToken = token;
    setStatus("Fyralis found the Slack configuration token. Creating the app...");
    const response = await submitToken(String(handoff.endpoint), token);
    if (!response.ok) {
      submittedToken = null;
      const detail =
        response.error ||
        (response.payload && (response.payload.detail || response.payload.error)) ||
        response.status ||
        "gateway rejected token";
      setStatus(`Fyralis could not create the Slack app: ${JSON.stringify(detail)}`);
      showPasteFallback(handoff, "Paste the generated Slack token here if it is visible.");
      return false;
    }
    const installUrl =
      response.payload &&
      (response.payload.install_url || response.payload.oauth_redirect_url);
    if (installUrl && /^https:\/\/slack\.com\/oauth\//.test(String(installUrl))) {
      setStatus("Slack app created. Opening OAuth approval...");
      window.location.assign(String(installUrl));
      return true;
    }
    setStatus("Slack app created. Return to Fyralis to verify the connection.");
    return true;
  }

  async function run() {
    let handoff = decodeHandoff();
    if (!handoff || handoff.source !== "slack" || !handoff.endpoint) {
      if (tokenFromDom()) {
        handoff = {
          source: "slack",
          endpoint: DEFAULT_LOCAL_ENDPOINT
        };
        setStatus("Fyralis found a Slack token. Using the local gateway handoff...");
      } else {
        return;
      }
    }
    setStatus("Fyralis is preparing Slack app creation...");
    let clickedGenerate = false;
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      if (closeWrongCreateAppDialog()) {
        clickedGenerate = false;
        await new Promise((resolve) => setTimeout(resolve, POLL_MS));
        continue;
      }
      const token = tokenFromDom();
      if (token) {
        if (await submitDetectedToken(handoff, token)) {
          return;
        }
      }
      const copiedToken = await tokenFromAccessTokenCopyButton();
      if (copiedToken) {
        if (await submitDetectedToken(handoff, copiedToken)) {
          return;
        }
      } else if (accessTokenCopyClicked && !fallbackShown) {
        showPasteFallback(
          handoff,
          "Fyralis clicked Access Token Copy, but Chrome blocked clipboard read. Click Read copied token, or paste the token here."
        );
      }
      if (!clickedGenerate) {
        clickedGenerate = clickGenerateToken();
        if (clickedGenerate) {
          setStatus("Fyralis clicked Generate Token. Waiting for Slack...");
        }
      } else {
        clickTokenDialogAction();
      }
      await new Promise((resolve) => setTimeout(resolve, POLL_MS));
    }
    showPasteFallback(
      handoff,
      "Fyralis could not read the token automatically. Paste the generated Slack token here."
    );
  }

  function start() {
    if (runInProgress) {
      return;
    }
    runInProgress = true;
    run()
      .catch((error) => {
        setStatus(`Fyralis browser agent stopped: ${String(error && error.message || error)}`);
      })
      .finally(() => {
        runInProgress = false;
      });
  }

  start();
  const observer = new MutationObserver(() => {
    if (!submittedToken && tokenFromDom()) {
      start();
    }
  });
  observer.observe(document.documentElement, {
    childList: true,
    subtree: true,
    characterData: true,
    attributes: true
  });
})();
