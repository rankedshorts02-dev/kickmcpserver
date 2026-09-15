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
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { z } from "zod";
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

const execFileAsync = promisify(execFile);

// Where downloaded videos are cached before being re-served. This is plain
// local disk on the Render instance — it does NOT persist across restarts
// or redeploys, and free-tier disk space is limited, so this is meant for
// short-lived relay (download, hand the URL to a clipping tool, done), not
// long-term storage. Use low/medium quality downloads to stay well within
// free-tier limits.
const VIDEOS_DIR = path.join(process.cwd(), "cached-videos");
fs.mkdirSync(VIDEOS_DIR, { recursive: true });

// The public base URL other services (OpusClip, Descript) will use to fetch
// cached files back from this server. Defaults to this deployment's known
// Render URL; override with PUBLIC_BASE_URL if the service is ever renamed.
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "https://kickmcpserver.onrender.com";

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
// file. This shells out to curl (present on Render's standard Node image)
// rather than using fetch(), because curl's TLS/HTTP fingerprint is less
// likely to get flagged by Kick's Cloudflare bot protection than Node's
// built-in client is — the same trick community Kick libraries use. This is
// still not a guaranteed bypass; if it starts failing again, that's Kick's
// edge tightening further, not a bug in your setup — the Apify "Kick
// Scraper" or "Kick All-in-One API" actors are maintained alternatives worth
// connecting as a fallback, since keeping up with anti-bot changes is their
// whole job, not a side effect of ours.
// ---------------------------------------------------------------------------

async function kickUnofficialListVideos(slug, limit = 10) {
  const url = `https://kick.com/api/v2/channels/${slug}/videos`;
  console.log(`[list_recent_videos] fetching ${url} via curl...`);

  let stdout;
  try {
    const result = await execFileAsync(
      "curl",
      [
        "-sL",
        "--max-time",
        "15",
        "-A",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "-H",
        "Accept: application/json",
        url,
      ],
      { timeout: 20_000, maxBuffer: 10 * 1024 * 1024 }
    );
    stdout = result.stdout;
    console.log(
      `[list_recent_videos] curl succeeded, ${stdout.length} bytes returned. First 200 chars: ${stdout.slice(0, 200)}`
    );
  } catch (err) {
    console.error(`[list_recent_videos] curl execution failed:`, err);
    throw new Error(
      `curl request to the unofficial videos endpoint failed: ${err.message}. ` +
        `If curl isn't installed in this environment, that's the real cause — ` +
        `switching to a maintained Apify scraper (Kick Scraper / Kick All-in-One ` +
        `API) is the more durable fix at that point.`
    );
  }

  let data;
  try {
    data = JSON.parse(stdout);
  } catch (parseErr) {
    console.error(
      `[list_recent_videos] response was not valid JSON. Parse error: ${parseErr.message}. Raw response (first 500 chars): ${stdout.slice(0, 500)}`
    );
    throw new Error(
      "The unofficial videos endpoint didn't return JSON (likely a Cloudflare " +
        "challenge page instead of data) — Kick's bot protection is still " +
        "blocking this request even via curl. Treat this tool as blocked for " +
        "now and use the Apify \"Kick Scraper\" or \"Kick All-in-One API\" " +
        "actors instead, which are maintained specifically to keep up with " +
        "this."
    );
  }

  const videos = Array.isArray(data) ? data : data.videos || data.data || [];
  console.log(`[list_recent_videos] parsed ${videos.length} video entries`);

  // Confirmed field names from a real response (Kick's fields, not documented
  // anywhere, so worth naming explicitly): `duration` is in MILLISECONDS,
  // and the real video identifier is nested at `video.uuid` — a top-level
  // `slug` also exists but is a separate SEO-style string, not the page URL.
  // `source` is a direct HLS stream manifest URL for the finished VOD, which
  // may be more directly usable by a clipping tool than the page URL is.
  return videos.slice(0, limit).map((v) => ({
    id: v.id ?? null,
    title: v.session_title ?? null,
    startedAt: v.created_at ?? null,
    durationSeconds: typeof v.duration === "number" ? Math.round(v.duration / 1000) : null,
    viewCount: v.views ?? v.viewer_count ?? null,
    pageUrl: v.video?.uuid ? `https://kick.com/${slug}/videos/${v.video.uuid}` : null,
    hlsStreamUrl: v.source ?? null,
  }));
}

// ---------------------------------------------------------------------------
// Cache-and-reserve: downloads a video from any URL (e.g. an Apify-hosted
// file) onto this server's local disk, then serves it back out through
// Express's static file handler, which supports HTTP range requests
// correctly by default. This exists because both OpusClip and Descript
// require range-request support to read a remote video, and Apify's
// key-value-store file hosting doesn't provide that — so the fix is to
// re-host the file somewhere that does, rather than anything about Kick
// or the video format itself.
// ---------------------------------------------------------------------------

// In-memory tracking for background downloads. Lost on restart, but that's
// fine — a lost job just needs to be re-started.
const downloadJobs = {};

function startBackgroundDownload(sourceUrl, filename) {
  const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, "_");
  const destPath = path.join(VIDEOS_DIR, `raw-${safeName}`);
  const trackingId = randomUUID();
  downloadJobs[trackingId] = { status: "running", localPath: destPath, error: null };

  // Deliberately NOT awaited — this is the fix. Awaiting the full download
  // inside a request handler ties it to that single incoming HTTP request's
  // lifetime, and Render appears to kill the underlying connection after
  // some platform-level time limit regardless of any timeout set in this
  // code, silently truncating large/long downloads with no thrown error.
  // Running it detached lets it continue as long as the Node process is
  // alive, independent of any one request.
  // Deliberately NOT awaited. Originally this used Node's fetch()+pipeline,
  // but that truncated large downloads at an identical byte count (~510MB)
  // on repeated attempts regardless of whether it ran synchronously or in
  // the background — ruling out a request-timeout theory and pointing to
  // something in Node's fetch implementation itself with very large
  // responses. curl is a mature, purpose-built tool for large file
  // transfers and doesn't share that limitation.
  (async () => {
    try {
      console.log(`[download_bg ${trackingId}] fetching via curl ${sourceUrl} -> ${destPath}`);
      await execFileAsync(
        "curl",
        ["-sL", "--fail", "-o", destPath, sourceUrl],
        { timeout: 0, maxBuffer: 10 * 1024 * 1024 }
      );
      const stats = fs.statSync(destPath);
      downloadJobs[trackingId] = { status: "done", localPath: destPath, bytesWritten: stats.size, error: null };
      console.log(`[download_bg ${trackingId}] done, ${stats.size} bytes`);
    } catch (err) {
      console.error(`[download_bg ${trackingId}] failed:`, err);
      downloadJobs[trackingId] = { status: "error", localPath: destPath, error: err.message };
    }
  })();

  return trackingId;
}

function getDownloadStatus(trackingId) {
  const job = downloadJobs[trackingId];
  if (!job) {
    throw new Error(`Unknown trackingId "${trackingId}" — it may be from before a server restart.`);
  }
  let currentBytesOnDisk = 0;
  try {
    currentBytesOnDisk = fs.statSync(job.localPath).size;
  } catch {
    // file may not exist yet
  }
  return { ...job, currentBytesOnDisk };
}

async function downloadRawVideo(sourceUrl, filename) {
  const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, "_");
  const destPath = path.join(VIDEOS_DIR, `raw-${safeName}`);

  console.log(`[download_raw] fetching ${sourceUrl} -> ${destPath}`);
  const res = await fetch(sourceUrl);
  if (!res.ok || !res.body) {
    throw new Error(`Failed to fetch source video: HTTP ${res.status}`);
  }

  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(destPath));
  const stats = fs.statSync(destPath);
  console.log(`[download_raw] done, ${stats.size} bytes written to ${destPath}`);

  // Returns a local path, not a public URL — this file is meant to be
  // chunked with extract_video_segment (which accepts a local path as its
  // sourceUrl, since ffmpeg -i handles local files the same as remote
  // ones), not served directly.
  return { localPath: destPath, bytesWritten: stats.size };
}

async function extractSegmentFromUrl(sourceUrl, filename, startOffsetSec, durationSec) {
  const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, "_");
  const destPath = path.join(VIDEOS_DIR, safeName);

  // Lets ffmpeg fetch directly from the source (HLS or a flat file) rather
  // than downloading the whole thing through Node first — for a multi-hour
  // source, this pulls only the segments needed for the requested window
  // instead of the entire file. -ss BEFORE -i is fast/input-side seeking.
  // -c copy avoids re-encoding (fast, no quality loss); if that ever
  // produces a broken cut at the boundary, re-encoding is the fallback but
  // isn't attempted automatically here.
  console.log(
    `[extract_segment] ffmpeg pulling ${sourceUrl} from ${startOffsetSec}s for ${durationSec}s -> ${destPath}`
  );
  // NOTE: stream-copy, not re-encode. Re-encoding was tried as a fix for
  // metadata issues when pulling directly from a *live* HLS manifest, but
  // it pins this instance's very limited CPU hard enough to make the whole
  // server unresponsive, even for a 3-minute test. The actual fix is
  // upstream: only ever point this at an already-finalized file (e.g. one
  // already downloaded via the Apify actor), never at Kick's raw live HLS
  // URL directly. Copying from a well-formed file doesn't carry the same
  // metadata corruption risk that copying from an in-progress live stream
  // does, so plain copy should be reliable here.
  try {
    await execFileAsync(
      "ffmpeg",
      [
        "-y",
        "-ss", String(startOffsetSec),
        "-i", sourceUrl,
        "-t", String(durationSec),
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        destPath,
      ],
      { timeout: 590_000, maxBuffer: 10 * 1024 * 1024 }
    );
  } catch (err) {
    console.error(`[extract_segment] ffmpeg failed:`, err);
    throw new Error(
      `ffmpeg couldn't extract this segment: ${err.message}. If the source ` +
        `is an HLS URL, it may need re-encoding instead of stream copy for ` +
        `a clean cut at this boundary, or the source URL itself may no ` +
        `longer be reachable.`
    );
  }

  const stats = fs.statSync(destPath);
  const publicUrl = `${PUBLIC_BASE_URL}/videos/${safeName}`;
  console.log(`[extract_segment] done, ${stats.size} bytes written, serving at ${publicUrl}`);

  return { publicUrl, bytesWritten: stats.size };
}

async function cacheVideoFromUrl(sourceUrl, filename, maxDurationSec) {
  // Keep filenames safe and predictable — strip anything that isn't
  // alphanumeric, dot, dash, or underscore.
  const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, "_");
  const rawPath = path.join(VIDEOS_DIR, `raw-${safeName}`);
  const destPath = path.join(VIDEOS_DIR, safeName);

  console.log(`[cache_video] fetching ${sourceUrl} -> ${rawPath}`);
  const res = await fetch(sourceUrl);
  if (!res.ok || !res.body) {
    throw new Error(`Failed to fetch source video: HTTP ${res.status}`);
  }

  // Stream straight to disk rather than buffering in memory — this file
  // can be hundreds of MB to multiple GB, and the instance only has 512MB
  // of RAM on the free tier.
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(rawPath));
  const rawStats = fs.statSync(rawPath);
  console.log(`[cache_video] raw download complete, ${rawStats.size} bytes`);

  // Videos remuxed from live HLS (which is what this download effectively
  // is) commonly end up with their metadata ("moov atom") at the END of the
  // file rather than the start. Tools that read metadata via a small range
  // request near the beginning — which is what both OpusClip and Descript's
  // errors pointed to — can't find it there and report the file as
  // unreadable/corrupt even though it's structurally valid. `-movflags
  // faststart` does a fast, lossless remux (no re-encoding) that moves the
  // moov atom to the front. If ffmpeg isn't available on this image, this
  // will throw clearly rather than silently, which itself is useful
  // information — it would mean this fix needs a different environment.
  //
  // maxDurationSec (optional) trims the output to that length using the
  // same ffmpeg pass — added because some downstream tools (OpusClip on a
  // trial plan) reject a source video based on its FULL original length
  // even when a processing range is requested separately, so the only
  // reliable fix is to hand over a genuinely shorter file.
  console.log(
    `[cache_video] running ffmpeg faststart remux${maxDurationSec ? ` (trimmed to ${maxDurationSec}s)` : ""}...`
  );
  const ffmpegArgs = ["-y", "-i", rawPath];
  if (maxDurationSec) {
    ffmpegArgs.push("-t", String(maxDurationSec));
  }
  ffmpegArgs.push("-c", "copy", "-movflags", "+faststart", destPath);

  try {
    await execFileAsync("ffmpeg", ffmpegArgs, { timeout: 180_000, maxBuffer: 10 * 1024 * 1024 });
  } catch (err) {
    console.error(`[cache_video] ffmpeg faststart remux failed:`, err);
    throw new Error(
      `Download succeeded but the faststart remux failed: ${err.message}. ` +
        `If ffmpeg isn't installed on this image, that's the real cause — ` +
        `serving the raw file without this fix is unlikely to work with ` +
        `tools that need to read metadata via range requests.`
    );
  } finally {
    fs.unlink(rawPath, () => {});
  }

  const stats = fs.statSync(destPath);
  const publicUrl = `${PUBLIC_BASE_URL}/videos/${safeName}`;
  console.log(`[cache_video] done, ${stats.size} bytes written after remux, serving at ${publicUrl}`);

  return { publicUrl, bytesWritten: stats.size };
}

// ---------------------------------------------------------------------------
// MCP server + tools
//
// IMPORTANT: the SDK only allows one active transport per McpServer instance
// — calling server.connect() a second time on the same instance throws
// "Already connected to a transport". Since Streamable HTTP can have multiple
// concurrent client sessions (Claude may reconnect, retry, etc.), each
// session needs its OWN McpServer instance. So tool registration lives in a
// factory function, called fresh per session below, instead of one shared
// top-level `server`.
// ---------------------------------------------------------------------------

function createMcpServer() {
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
        "Lists a channel's most recent past broadcasts (VODs), with a page URL and a " +
        "direct HLS stream URL for each. Uses Kick's undocumented internal endpoint " +
        "since there is no official video-listing API — treat results as best-effort " +
        "and expect this to occasionally need maintenance if Kick changes their site.",
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

  server.registerTool(
    "cache_video_for_processing",
    {
      title: "Cache a remote video for tools that need range-request support",
      description:
        "Downloads a video from any URL (e.g. an Apify-hosted file), remuxes it " +
        "with ffmpeg -movflags faststart (fixes metadata placement for videos " +
        "remuxed from live HLS, without re-encoding), and re-serves it with " +
        "proper HTTP range-request support — both of which OpusClip and " +
        "Descript need but Apify's file hosting/raw remux don't provide. " +
        "Optionally trims to maxDurationSec in the same pass — useful when a " +
        "downstream tool rejects a video based on its full original length " +
        "even when a separate processing range was requested (seen with " +
        "OpusClip on long source videos). Returns a new URL to feed into " +
        "those tools instead of the original one. Note: the cached file is " +
        "temporary and does not survive a server restart/redeploy.",
      inputSchema: {
        sourceUrl: z.string().describe("URL of the video to download and re-host"),
        filename: z.string().describe("Filename to save it as, e.g. 'n3on-vod-1.mp4'"),
        maxDurationSec: z
          .number()
          .int()
          .positive()
          .optional()
          .describe("Optional: trim the output to this many seconds from the start"),
      },
    },
    async ({ sourceUrl, filename, maxDurationSec }) => {
      const result = await cacheVideoFromUrl(sourceUrl, filename, maxDurationSec);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }
  );

  server.registerTool(
    "extract_video_segment",
    {
      title: "Extract a time-bounded segment directly from a video URL",
      description:
        "Pulls a specific time window (startOffsetSec to startOffsetSec+durationSec) " +
        "directly from a source URL — such as Kick's own HLS stream URL from " +
        "list_recent_videos — via ffmpeg, without downloading the entire source " +
        "first. Built for splitting a multi-hour VOD into chunks (e.g. for " +
        "submitting each chunk to OpusClip separately) without storing the " +
        "full file on disk. Output is faststart-remuxed and range-request " +
        "servable, same as cache_video_for_processing. Note: served files are " +
        "temporary and do not survive a server restart/redeploy.",
      inputSchema: {
        sourceUrl: z.string().describe("Source video URL, e.g. an HLS master.m3u8 URL"),
        filename: z.string().describe("Filename to save the segment as, e.g. 'stream-chunk-1.mp4'"),
        startOffsetSec: z.number().nonnegative().describe("Start offset in seconds from the beginning of the source"),
        durationSec: z.number().positive().describe("How many seconds to extract from that offset"),
      },
    },
    async ({ sourceUrl, filename, startOffsetSec, durationSec }) => {
      const result = await extractSegmentFromUrl(sourceUrl, filename, startOffsetSec, durationSec);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }
  );

  server.registerTool(
    "download_raw_video",
    {
      title: "Download and persist a raw video file locally",
      description:
        "Downloads a file (e.g. an Apify-hosted download URL) onto this server's " +
        "disk and keeps it there (unlike cache_video_for_processing, which " +
        "deletes the raw download after processing it). WARNING: only safe for " +
        "smaller/quick downloads — Render appears to enforce a per-request time " +
        "limit that silently truncates larger downloads run this way, with no " +
        "error thrown (confirmed: a 9-hour video came back as a truncated ~500MB " +
        "file with no warning). For anything of meaningful size or a long " +
        "stream, use start_background_download + check_download_status instead. " +
        "Returns a local file path, not a public URL — meant to be reused as " +
        "the sourceUrl for multiple extract_video_segment calls (chunking a " +
        "long video into several pieces) without re-downloading for each " +
        "chunk. Note: the file is temporary and does not survive a server " +
        "restart/redeploy, and counts toward this instance's limited free-tier " +
        "disk space.",
      inputSchema: {
        sourceUrl: z.string().describe("URL of the file to download"),
        filename: z.string().describe("Filename to save it as, e.g. 'trainwreck-full.mp4'"),
      },
    },
    async ({ sourceUrl, filename }) => {
      const result = await downloadRawVideo(sourceUrl, filename);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }
  );

  server.registerTool(
    "start_background_download",
    {
      title: "Start a large download in the background",
      description:
        "Starts downloading a file (e.g. an Apify-hosted video) WITHOUT waiting " +
        "for it to finish, and returns immediately with a trackingId. Use this " +
        "instead of download_raw_video for anything large/long — Render appears " +
        "to enforce a per-request time limit that silently truncates downloads " +
        "run synchronously within a single request, with no error thrown. Poll " +
        "check_download_status with the returned trackingId until status is " +
        "'done', then use the localPath it reports as the sourceUrl for " +
        "extract_video_segment.",
      inputSchema: {
        sourceUrl: z.string().describe("URL of the file to download"),
        filename: z.string().describe("Filename to save it as, e.g. 'trainwreck-full.mp4'"),
      },
    },
    async ({ sourceUrl, filename }) => {
      const trackingId = startBackgroundDownload(sourceUrl, filename);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              { trackingId, message: "Download started in the background. Poll check_download_status to see progress." },
              null,
              2
            ),
          },
        ],
      };
    }
  );

  server.registerTool(
    "check_download_status",
    {
      title: "Check on a background download",
      description:
        "Checks the status of a download started with start_background_download. " +
        "Returns status ('running' | 'done' | 'error'), the current file size on " +
        "disk (useful for gauging progress on a still-running download), and — " +
        "once done — the localPath to use as extract_video_segment's sourceUrl.",
      inputSchema: {
        trackingId: z.string().describe("The trackingId returned by start_background_download"),
      },
    },
    async ({ trackingId }) => {
      const status = getDownloadStatus(trackingId);
      return { content: [{ type: "text", text: JSON.stringify(status, null, 2) }] };
    }
  );

  return server;
}

// ---------------------------------------------------------------------------
// Streamable HTTP transport (what lets this be added as a Claude custom
// connector via URL, rather than run only as a local stdio process)
// ---------------------------------------------------------------------------

const app = express();
app.use(express.json());

const transports = {};

app.post("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  console.log(
    `[mcp] incoming request, session: ${sessionId || "(none)"}, method: ${req.body?.method || "(unknown)"}`
  );
  let transport = sessionId ? transports[sessionId] : undefined;

  if (!transport) {
    const isInitializeRequest = req.body?.method === "initialize";

    // A session ID was sent, but we don't recognize it — almost always because
    // Render restarted the process (e.g. a redeploy) and wiped the in-memory
    // `transports` map, while Claude's client is still holding onto a session
    // ID from before the restart. Silently trying to reuse it causes the
    // request to just hang with no error. Instead, explicitly tell the client
    // the session is gone so it reinitializes a fresh one.
    if (sessionId && !isInitializeRequest) {
      console.warn(`[mcp] unknown session ${sessionId} for method ${req.body?.method} — likely a stale session from before a restart. Telling client to reinitialize.`);
      res.status(400).json({
        jsonrpc: "2.0",
        error: { code: -32000, message: "Session not found. Please reinitialize." },
        id: req.body?.id ?? null,
      });
      return;
    }

    transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sid) => {
        transports[sid] = transport;
      },
    });
    transport.onclose = () => {
      if (transport.sessionId) delete transports[transport.sessionId];
    };
    const server = createMcpServer();
    await server.connect(transport);
  }

  await transport.handleRequest(req, res, req.body);
});

// Serves cached videos with correct HTTP range-request support out of the
// box — this is the whole point of the cache_video_for_processing tool above.
app.use("/videos", express.static(VIDEOS_DIR));

app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`kick-mcp-server listening on :${PORT} (POST /mcp)`);
});
