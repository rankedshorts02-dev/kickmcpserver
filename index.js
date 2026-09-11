// kick-mcp-server
//
// Exposes Kick.com data as MCP tools over Streamable HTTP, so it can be added
// to Claude as a custom connector (Settings -> Connectors -> Add custom
// connector -> paste this server's public URL + "/mcp").
//
// Two tiers of API here, and they are NOT equally reliable:
//
//  1. OFFICIAL  (api.kick.com/public/v1) - channel lookup + live status.
//     Documented, stable, requires a Kick developer app (client id/secret).
//
//  2. UNOFFICIAL (kick.com/api/v2/...) - recent VODs. Kick does not publish
//     a video-listing endpoint on its public API. Every third-party tool
//     that lists VODs (Rust/Go/.NET community libraries, Apify scrapers)
//     reverse-engineers this same internal endpoint, and all of them warn
//     it can change or break without notice. Treat `list_recent_videos`
//     as best-effort. If it stops working, that's Kick changing something
//     on their end, not a bug in your setup - the Apify "Kick Clip
//     Downloader" / "Kick Scraper" actors are maintained alternatives that
//     track those changes for you.

import express from "express";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

const KICK_CLIENT_ID = process.env.KICK_CLIENT_ID;
const KICK_CLIENT_SECRET = process.env.KICK_CLIENT_SECRET;
const PORT = process.env.PORT || 3000;

if (!KICK_CLIENT_ID || !KICK_CLIENT_SECRET) {
  console.error(
    "Missing KICK_CLIENT_ID / KICK_CLIENT_SECRET. Create an app at " +
      "https://kick.com/settings/developer and set these in your .env."
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Official API: app access token (client_credentials grant), cached + refreshed
// ---------------------------------------------------------------------------

let cachedToken = null; // { accessToken, expiresAt }

async function getAppAccessToken() {
  if (cachedToken && cachedToken.expiresAt > Date.now() + 30_000) {
    return cachedToken.accessToken;
  }

  const res = await fetch("https://id.kick.com/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: KICK_CLIENT_ID,
      client_secret: KICK_CLIENT_SECRET,
    }),
  });

  if (!res.ok) {
    throw new Error(`Kick token request failed (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  cachedToken = {
    accessToken: data.access_token,
    expiresAt: Date.now() + data.expires_in * 1000,
  };
  return cachedToken.accessToken;
}

async function kickOfficialFetch(path) {
  const token = await getAppAccessToken();
  const res = await fetch(`https://api.kick.com/public/v1${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    throw new Error(`Kick API error (${res.status}) for ${path}: ${await res.text()}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Unofficial: recent videos. Best-effort — see the warning at the top of this
// file. A plain browser User-Agent is set because Kick's edge (Cloudflare)
// sometimes blocks obvious bot/script traffic; this is NOT a full bypass, so
// if this starts failing with 403s, that's expected and you're not missing a
// config step, Kick's edge has just tightened up.
// ---------------------------------------------------------------------------

async function kickUnofficialListVideos(slug, limit = 10) {
  const res = await fetch(`https://kick.com/api/v2/channels/${slug}/videos`, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      Accept: "application/json",
    },
  });

  if (!res.ok) {
    throw new Error(
      `Unofficial videos endpoint returned ${res.status}. This endpoint is ` +
        `undocumented and known to be fragile — if it's newly broken, try the ` +
        `Apify "Kick Clip Downloader" or "Kick Scraper" actors as a fallback.`
    );
  }

  const data = await res.json();
  const videos = Array.isArray(data) ? data : data.videos || data.data || [];

  return videos.slice(0, limit).map((v) => ({
    id: v.id ?? v.uuid ?? null,
    title: v.session_title ?? v.title ?? null,
    startedAt: v.started_at ?? v.created_at ?? null,
    durationSeconds: v.duration_seconds ?? v.duration ?? null,
    viewCount: v.view_count ?? v.views ?? null,
    url: v.url ?? (v.uuid ? `https://kick.com/${slug}/videos/${v.uuid}` : null),
  }));
}

// ---------------------------------------------------------------------------
// MCP server + tools
// ---------------------------------------------------------------------------

const server = new McpServer({ name: "kick-mcp", version: "0.1.0" });

server.registerTool(
  "get_channel_info",
  {
    title: "Get Kick channel info",
    description:
      "Look up a Kick.com channel by slug (the name in the URL, e.g. 'xqc'). " +
      "Returns live status, category, and basic channel details via Kick's official API.",
    inputSchema: {
      slug: z.string().describe("Kick channel slug, e.g. 'xqc' from kick.com/xqc"),
    },
  },
  async ({ slug }) => {
    const data = await kickOfficialFetch(`/channels?slug=${encodeURIComponent(slug)}`);
    const channel = data?.data?.[0] ?? null;
    if (!channel) {
      return {
        content: [{ type: "text", text: `No channel found for slug "${slug}".` }],
      };
    }
    return { content: [{ type: "text", text: JSON.stringify(channel, null, 2) }] };
  }
);

server.registerTool(
  "list_recent_videos",
  {
    title: "List recent Kick VODs (best-effort)",
    description:
      "Lists a channel's most recent past broadcasts (VODs). Uses Kick's " +
      "undocumented internal endpoint since there is no official video-listing " +
      "API — treat results as best-effort and expect this to occasionally need " +
      "maintenance if Kick changes their site.",
    inputSchema: {
      slug: z.string().describe("Kick channel slug, e.g. 'xqc'"),
      limit: z.number().int().min(1).max(25).default(10).describe("Max videos to return"),
    },
  },
  async ({ slug, limit }) => {
    const videos = await kickUnofficialListVideos(slug, limit);
    return { content: [{ type: "text", text: JSON.stringify(videos, null, 2) }] };
  }
);

// ---------------------------------------------------------------------------
// Streamable HTTP transport (what lets this be added as a Claude custom
// connector via URL, rather than run only as a local stdio process)
// ---------------------------------------------------------------------------

const app = express();
app.use(express.json());

const transports = {};

app.post("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  let transport = sessionId ? transports[sessionId] : undefined;

  if (!transport) {
    transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sid) => {
        transports[sid] = transport;
      },
    });
    transport.onclose = () => {
      if (transport.sessionId) delete transports[transport.sessionId];
    };
    await server.connect(transport);
  }

  await transport.handleRequest(req, res, req.body);
});

app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`kick-mcp-server listening on :${PORT} (POST /mcp)`);
});
