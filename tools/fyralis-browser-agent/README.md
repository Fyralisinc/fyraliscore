# Fyralis Browser Agent

Local unpacked Chrome helper for BYOC provider setup tests.

Load it from `chrome://extensions` with Developer mode enabled, then click
**Load unpacked** and select this folder:

```text
tools/fyralis-browser-agent
```

For Slack, the Fyralis UI opens `https://api.slack.com/apps` with a handoff
fragment. This helper reads that fragment, generates or finds the Slack app
configuration token in the logged-in Slack page, posts it to the local Fyralis
gateway, and opens Slack OAuth if the gateway creates the app successfully.

The helper only posts to:

- `http://localhost:8000`
- `http://127.0.0.1:8000`
