# kick-mcp-server

A small MCP server for Kick.com, built for the "watch Neon's channel, feed VODs
to OpusClip" workflow. Two tools:

- **`get_channel_info`** — official API, stable. Live status, category, basics.
- **`list_recent_videos`** — best-effort. Kick has no official endpoint for
  listing past broadcasts, so this uses the same undocumented internal
  endpoint every third-party Kick tool relies on. It can break if Kick
  changes their site without notice. If it does, the Apify **"Kick Clip
  Downloader"** or **"Kick Scraper"** actors are maintained fallbacks that
  someone else keeps patched against those changes.

## 1. Get Kick API credentials

1. Go to https://kick.com/settings/developer and create an application.
2. Copy the **Client ID** and **Client Secret**.

You don't need a redirect URI for this — the server only uses the
`client_credentials` grant (app-level, read-only public data), not user login.

## 2. Configure

```bash
cp .env.example .env
# then fill in KICK_CLIENT_ID and KICK_CLIENT_SECRET in .env
```

## 3. Run locally

```bash
npm install
npm start
```

This starts the server on `http://localhost:3000` with the MCP endpoint at
`POST /mcp`.

## 4. Make it reachable from Claude

Claude's web/mobile app connects to **remote** MCP servers over HTTPS — it
can't reach `localhost` on your machine. You have two options:

**Quick test:** use a tunnel like `ngrok http 3000` to get a temporary public
HTTPS URL. Good for trying it out, but the URL changes every time you restart
the tunnel.

**Real deployment:** deploy this folder to a small always-on host — Render,
Railway, and Fly.io all have free or near-free tiers that work fine for this.
Set `KICK_CLIENT_ID` and `KICK_CLIENT_SECRET` as environment variables in
whichever host you pick; they set `PORT` automatically.

## 5. Add it to Claude

In Claude: **Settings → Connectors → Add custom connector**, then paste your
server's URL with `/mcp` on the end, e.g. `https://your-app.onrender.com/mcp`.

## Notes / limitations

- `list_recent_videos` sets a browser-like User-Agent to avoid the most basic
  bot blocking, but this is not a full Cloudflare bypass. If it starts
  returning 403s, that's Kick's edge tightening up, not a bug in your
  config — see the fallback note above.
- The video-list response shape is inferred from public reverse-engineering
  write-ups, not from Kick documentation (none exists for this endpoint).
  Field names in the output are normalized as best-effort; if Kick's actual
  response differs, the raw fields may need remapping in `index.js`.
- This was written without the ability to `npm install` or run it against a
  live Kick app in the environment it was built in, so treat the first run as
  a shakedown — if you hit an error, paste it back and it can be fixed.
