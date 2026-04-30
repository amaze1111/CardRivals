// Revised server.js copy you can paste into your backend.
// Requires: keep `backend_matchmaking.js` in the same folder.
//
// Key changes:
// - Real matchmaking queue via `backend_matchmaking.js`
// - Adds `GET /matchmaking/status?matchId=...` to return opponent display names
// - Fixes `/matchmaking/join` to pass `displayName` and to not pre-generate matchId
//
// NOTE: This file is based on the full server.js you pasted in chat.

import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";
import pg from "pg";
import bcrypt from "bcryptjs";
import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { createMatchmakingManager } from "./backend_matchmaking.js";

const { Pool } = pg;

const PORT = Number(process.env.PORT || 8080);
const DATABASE_URL = process.env.DATABASE_URL;
const FIREBASE_SERVICE_ACCOUNT_JSON = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "";
const STARTING_COINS = Number(process.env.STARTING_COINS || 500);
const DAILY_COIN_REWARD = Number(process.env.DAILY_COIN_REWARD || 75);
const MATCH_ENTRY_FEES = {
  2: Number(process.env.MATCH_ENTRY_FEE_2P || 1),
  4: Number(process.env.MATCH_ENTRY_FEE_4P || 40),
};

const SUITS = ["\u2660", "\u2665", "\u2666", "\u2663"];
const RANKS = [
  ["A", 14], ["2", 2], ["3", 3], ["4", 4], ["5", 5], ["6", 6], ["7", 7],
  ["8", 8], ["9", 9], ["10", 10], ["J", 11], ["Q", 12], ["K", 13],
];

const QUEST_DEFINITIONS = [
  { key: "play_3_matches", title: "Play 3 Matches", target: 3, rewardCoins: 40, eventName: "game_completed" },
  { key: "win_1_reveal", title: "Win 1 Reveal", target: 1, rewardCoins: 30, eventName: "round_completed" },
  { key: "use_sort_2", title: "Use Sort 2 Times", target: 2, rewardCoins: 20, eventName: "sort_used" },
];

if (!DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Database-backed endpoints will fail until configured.");
}

const db = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : false,
});

const sessions = new Map();
const rooms = new Map();
const clientMeta = new Map();
const matchmakingSockets = new Map(); // userId -> WebSocket

function connectedCount(roomId) {
  let count = 0;
  clientMeta.forEach((meta, socket) => {
    if (socket.readyState !== 1) return;
    if (meta.roomId !== roomId) return;
    count += 1;
  });
  return count;
}

function ensureMatchRoom(matchId) {
  const normalized = String(matchId || "").trim().toUpperCase();
  if (!normalized) return null;
  const existing = rooms.get(normalized);
  if (existing) return existing;
  const status = matchmaking.getStatus({ matchId: normalized });
  if (!status) return null;
  const room = {
    id: normalized,
    phase: "lobby",
    round: 1,
    totalRounds: 3,
    activeArrangePlayerIndex: 0,
    battleGroupIndex: 0,
    battleRevealCount: 0,
    scores: status.players.map(() => 0),
    groupsWon: status.players.map(() => 0),
    players: status.players.map((p, index) => createPlayer(p.displayName || `Player ${index + 1}`, index)),
  };
  rooms.set(normalized, room);
  return room;
}

function attachSocketToMatchRoom({ socket, matchId, userId }) {
  const status = matchmaking.getStatus({ matchId });
  if (!status) {
    socketFail(socket, "Match not found.");
    return;
  }
  const playerIndex = status.players.findIndex((p) => p.userId === userId);
  if (playerIndex < 0) {
    socketFail(socket, "Not a participant in this match.");
    return;
  }
  const room = ensureMatchRoom(matchId);
  if (!room) {
    socketFail(socket, "Room not found.");
    return;
  }
  clientMeta.set(socket, { roomId: room.id, playerIndex });
  send(socket, "match_joined", { roomId: room.id, playerIndex, matchId: room.id });
  broadcastRoom(room);

  // Auto-start once all players are connected.
  if (room.phase === "lobby" && connectedCount(room.id) >= status.players.length) {
    room.scores = room.players.map(() => 0);
    room.groupsWon = room.players.map(() => 0);
    room.round = 1;
    dealRound(room);
    broadcastRoom(room);
  }
}

const matchmaking = createMatchmakingManager({
  waitMs: Number(process.env.MATCHMAKING_WAIT_MS || 5000),
  onRespond: ({ userId, payload }) => {
    if (!payload || !payload.matchId) return;
    if (payload.botFillApplied) return;
    const matchId = String(payload.matchId).trim().toUpperCase();
    const status = matchmaking.getStatus({ matchId });
    if (!status) return;

    // Ensure room exists and auto-attach any listening sockets.
    ensureMatchRoom(matchId);
    for (const player of status.players) {
      const socket = matchmakingSockets.get(player.userId);
      if (!socket || socket.readyState !== 1) continue;
      attachSocketToMatchRoom({ socket, matchId, userId: player.userId });
    }

    // Optional: analytics for match assignment.
    writeAnalyticsEvent({
      userId,
      eventName: "match_assigned",
      source: "server",
      matchId,
      payload: { playerCount: payload.playerCount },
    }).catch(() => {});
  },
});

const firebaseAuth = initializeFirebaseAdmin();

function initializeFirebaseAdmin() {
  if (!FIREBASE_SERVICE_ACCOUNT_JSON) {
    console.warn("FIREBASE_SERVICE_ACCOUNT_JSON is not configured. /auth/firebase will fail until it is set.");
    return null;
  }
  try {
    const raw = FIREBASE_SERVICE_ACCOUNT_JSON.trim().startsWith("{")
      ? FIREBASE_SERVICE_ACCOUNT_JSON
      : Buffer.from(FIREBASE_SERVICE_ACCOUNT_JSON, "base64").toString("utf8");
    const serviceAccount = JSON.parse(raw);
    if (getApps().length === 0) {
      initializeApp({ credential: cert(serviceAccount) });
    }
    return getAuth();
  } catch (error) {
    console.error("Failed to initialize Firebase Admin.", error);
    return null;
  }
}

function json(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
  });
  res.end(JSON.stringify(payload));
}

function fail(res, statusCode, message) {
  json(res, statusCode, { error: message });
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      if (!body) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function getBearerToken(req) {
  const header = req.headers.authorization || "";
  const [scheme, token] = header.split(" ");
  if (scheme?.toLowerCase() !== "bearer" || !token) return "";
  return token.trim();
}

function issueToken() {
  return crypto.randomBytes(24).toString("hex");
}

function normalizeProvider(provider) {
  return String(provider || "email").trim().toLowerCase() || "email";
}

function todayKey(date = new Date()) {
  return new Date(date).toISOString().slice(0, 10);
}

function computeRankTier(coins) {
  if (coins >= 3000) return "Diamond";
  if (coins >= 2000) return "Platinum";
  if (coins >= 1200) return "Gold";
  if (coins >= 700) return "Silver";
  return "Bronze";
}

function buildDisplayName(user) {
  return user.display_name || user.email || "Player";
}

async function ensureSchema() {
  if (!DATABASE_URL) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      firebase_uid TEXT UNIQUE,
      display_name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      photo_url TEXT,
      auth_provider TEXT NOT NULL DEFAULT 'email',
      password_hash TEXT,
      coins INTEGER NOT NULL DEFAULT ${STARTING_COINS},
      rank_tier TEXT NOT NULL DEFAULT 'Bronze',
      win_streak INTEGER NOT NULL DEFAULT 0,
      last_daily_claim_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  const columnsResult = await db.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users'
  `);
  const columns = new Set(columnsResult.rows.map((row) => row.column_name));
  const idTypeResult = await db.query(`
    SELECT data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'id'
  `);
  const userIdColumnType = idTypeResult.rowCount
    ? (idTypeResult.rows[0].udt_name === "uuid" ? "UUID" : "INTEGER")
    : "INTEGER";
  if (!columns.has("display_name")) {
    if (columns.has("displayname")) {
      await db.query(`ALTER TABLE users RENAME COLUMN displayname TO display_name`);
    } else {
      await db.query(`ALTER TABLE users ADD COLUMN display_name TEXT`);
      await db.query(`UPDATE users SET display_name = COALESCE(email, 'Player') WHERE display_name IS NULL`);
      await db.query(`ALTER TABLE users ALTER COLUMN display_name SET NOT NULL`);
    }
  }
  if (!columns.has("password_hash") && columns.has("password")) {
    await db.query(`ALTER TABLE users RENAME COLUMN password TO password_hash`);
  }
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS firebase_uid TEXT`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url TEXT`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider TEXT NOT NULL DEFAULT 'email'`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS coins INTEGER NOT NULL DEFAULT ${STARTING_COINS}`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS rank_tier TEXT NOT NULL DEFAULT 'Bronze'`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS win_streak INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_claim_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
  await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
  await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS users_email_key ON users(email)`);
  await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS users_firebase_uid_key ON users(firebase_uid) WHERE firebase_uid IS NOT NULL`);

  await db.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id ${userIdColumnType} NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at TIMESTAMPTZ,
      revoked_at TIMESTAMPTZ
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS analytics_events (
      id BIGSERIAL PRIMARY KEY,
      user_id ${userIdColumnType} REFERENCES users(id) ON DELETE SET NULL,
      event_name TEXT NOT NULL,
      source TEXT NOT NULL,
      match_id TEXT,
      payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS economy_transactions (
      id BIGSERIAL PRIMARY KEY,
      user_id ${userIdColumnType} NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      transaction_type TEXT NOT NULL,
      amount INTEGER NOT NULL,
      balance_after INTEGER NOT NULL,
      idempotency_key TEXT,
      metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS economy_transactions_idempotency_key
    ON economy_transactions(idempotency_key)
    WHERE idempotency_key IS NOT NULL
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS user_quests (
      user_id ${userIdColumnType} NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      quest_key TEXT NOT NULL,
      progress INTEGER NOT NULL DEFAULT 0,
      completed_at TIMESTAMPTZ,
      claimed_at TIMESTAMPTZ,
      PRIMARY KEY (user_id, quest_key)
    )
  `);
}

async function updateQuestProgress(userId, eventName, payload = {}) {
  const quests = QUEST_DEFINITIONS.filter((quest) => quest.eventName === eventName);
  if (eventName === "round_completed" && payload.result !== "win") return;
  for (const quest of quests) {
    await db.query(
      `
      INSERT INTO user_quests (user_id, quest_key, progress, completed_at, claimed_at)
      VALUES ($1, $2, 1, CASE WHEN 1 >= $3 THEN NOW() ELSE NULL END, NULL)
      ON CONFLICT (user_id, quest_key)
      DO UPDATE SET
        progress = CASE
          WHEN user_quests.claimed_at IS NOT NULL THEN user_quests.progress
          WHEN user_quests.progress >= $3 THEN user_quests.progress
          ELSE user_quests.progress + 1
        END,
        completed_at = CASE
          WHEN user_quests.claimed_at IS NOT NULL THEN user_quests.completed_at
          WHEN user_quests.progress + 1 >= $3 AND user_quests.completed_at IS NULL THEN NOW()
          ELSE user_quests.completed_at
        END
      `,
      [userId, quest.key, quest.target],
    );
  }
}

async function writeAnalyticsEvent({ userId = null, eventName, source, matchId = null, payload = {} }) {
  if (!DATABASE_URL) return;
  await db.query(
    `INSERT INTO analytics_events (user_id, event_name, source, match_id, payload_json)
     VALUES ($1, $2, $3, $4, $5::jsonb)`,
    [userId, eventName, source, matchId, JSON.stringify(payload || {})],
  );
  if (userId) {
    await updateQuestProgress(userId, eventName, payload);
  }
}

async function fetchQuestState(userId) {
  const result = await db.query(
    `SELECT quest_key, progress, completed_at, claimed_at FROM user_quests WHERE user_id = $1`,
    [userId],
  );
  const byKey = new Map(result.rows.map((row) => [row.quest_key, row]));
  return QUEST_DEFINITIONS.map((quest) => {
    const row = byKey.get(quest.key);
    const progress = Math.min(row?.progress || 0, quest.target);
    return {
      key: quest.key,
      title: quest.title,
      target: quest.target,
      rewardCoins: quest.rewardCoins,
      progress,
      completed: progress >= quest.target,
      claimed: Boolean(row?.claimed_at),
    };
  });
}

async function fetchUserByToken(token) {
  if (!token) return null;
  const cached = sessions.get(token);
  if (cached) return cached;
  const result = await db.query(
    `
    SELECT s.token, s.user_id, s.revoked_at, u.*
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.token = $1
    `,
    [token],
  );
  if (!result.rowCount || result.rows[0].revoked_at) return null;
  const row = result.rows[0];
  const session = {
    token,
    userId: row.user_id,
    displayName: row.display_name,
    email: row.email,
    authProvider: row.auth_provider,
    firebaseUid: row.firebase_uid || "",
  };
  sessions.set(token, session);
  return session;
}

async function requireAuth(req, res) {
  const token = getBearerToken(req);
  if (!token) {
    fail(res, 401, "Missing auth token.");
    return null;
  }
  const session = await fetchUserByToken(token);
  if (!session) {
    fail(res, 401, "Invalid or expired auth token.");
    return null;
  }
  return session;
}

async function fetchUserProfile(userId) {
  const result = await db.query(
    `SELECT id, firebase_uid, display_name, email, photo_url, auth_provider, coins, rank_tier, win_streak
     FROM users WHERE id = $1`,
    [userId],
  );
  if (!result.rowCount) return null;
  const user = result.rows[0];
  return {
    user: {
      id: String(user.id),
      firebaseUid: user.firebase_uid || "",
      displayName: buildDisplayName(user),
      email: user.email,
      photoUrl: user.photo_url || "",
      authProvider: user.auth_provider || "email",
    },
    wallet: {
      coins: user.coins,
      rankTier: user.rank_tier,
      winStreak: user.win_streak,
    },
    quests: await fetchQuestState(userId),
  };
}

async function issueAppSession(user) {
  const token = issueToken();
  await db.query(
    `INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, NOW() + INTERVAL '30 days')`,
    [token, user.id],
  );
  sessions.set(token, {
    token,
    userId: user.id,
    displayName: buildDisplayName(user),
    email: user.email,
    authProvider: user.auth_provider || "email",
    firebaseUid: user.firebase_uid || "",
  });
  await writeAnalyticsEvent({
    userId: user.id,
    eventName: "session_created",
    source: "server",
    payload: { provider: user.auth_provider || "email" },
  });
  await writeAnalyticsEvent({
    userId: user.id,
    eventName: "app_open",
    source: "server",
    payload: { provider: user.auth_provider || "email" },
  });
  return token;
}

async function revokeAppSession(token) {
  if (!token) return;
  sessions.delete(token);
  await db.query(`UPDATE sessions SET revoked_at = NOW() WHERE token = $1`, [token]);
}

async function buildAuthResponse(user, created) {
  const appToken = await issueAppSession(user);
  const profile = await fetchUserProfile(user.id);
  return {
    appToken,
    newUser: created,
    ...profile,
  };
}

async function resolveOrCreateFirebaseUser(decodedToken, providerHint = "firebase") {
  const firebaseUid = decodedToken.uid;
  const email = String(decodedToken.email || "").trim().toLowerCase();
  const displayName = String(decodedToken.name || email || "Player").trim();
  const photoUrl = decodedToken.picture || null;
  const authProvider = normalizeProvider(
    providerHint || decodedToken.firebase?.sign_in_provider || decodedToken.sign_in_provider || "firebase",
  );

  if (!email) {
    throw new Error("Firebase account is missing an email address.");
  }

  let result = await db.query(`SELECT * FROM users WHERE firebase_uid = $1`, [firebaseUid]);
  if (result.rowCount) {
    const user = result.rows[0];
    result = await db.query(
      `
      UPDATE users
      SET display_name = $2,
          email = $3,
          photo_url = $4,
          auth_provider = $5,
          last_login_at = NOW(),
          rank_tier = $6
      WHERE id = $1
      RETURNING *
      `,
      [user.id, displayName, email, photoUrl, authProvider, computeRankTier(user.coins ?? STARTING_COINS)],
    );
    return { user: result.rows[0], created: false };
  }

  result = await db.query(`SELECT * FROM users WHERE email = $1`, [email]);
  if (result.rowCount) {
    const user = result.rows[0];
    result = await db.query(
      `
      UPDATE users
      SET firebase_uid = COALESCE(firebase_uid, $2),
          display_name = COALESCE(NULLIF($3, ''), display_name),
          photo_url = COALESCE($4, photo_url),
          auth_provider = $5,
          last_login_at = NOW(),
          rank_tier = $6
      WHERE id = $1
      RETURNING *
      `,
      [user.id, firebaseUid, displayName, photoUrl, authProvider, computeRankTier(user.coins ?? STARTING_COINS)],
    );
    return { user: result.rows[0], created: false };
  }

  result = await db.query(
    `
    INSERT INTO users (firebase_uid, display_name, email, photo_url, auth_provider, coins, rank_tier, win_streak)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 0)
    RETURNING *
    `,
    [firebaseUid, displayName, email, photoUrl, authProvider, STARTING_COINS, computeRankTier(STARTING_COINS)],
  );
  return { user: result.rows[0], created: true };
}

async function ensureLegacyUser(displayName, email, password, isSignup) {
  const normalizedEmail = String(email).trim().toLowerCase();
  const normalizedName = String(displayName || "").trim() || normalizedEmail || "Player";

  if (isSignup) {
    const existing = await db.query(`SELECT id FROM users WHERE email = $1`, [normalizedEmail]);
    if (existing.rowCount) throw new Error("An account already exists for that email.");
    const passwordHash = await bcrypt.hash(String(password), 10);
    const result = await db.query(
      `
      INSERT INTO users (display_name, email, password_hash, auth_provider, coins, rank_tier)
      VALUES ($1, $2, $3, 'email', $4, $5)
      RETURNING *
      `,
      [normalizedName, normalizedEmail, passwordHash, STARTING_COINS, computeRankTier(STARTING_COINS)],
    );
    return { user: result.rows[0], created: true };
  }

  const result = await db.query(`SELECT * FROM users WHERE email = $1`, [normalizedEmail]);
  if (!result.rowCount) throw new Error("Invalid email or password.");
  const user = result.rows[0];
  if (!user.password_hash || !(await bcrypt.compare(String(password), user.password_hash))) {
    throw new Error("Invalid email or password.");
  }
  const updated = await db.query(
    `
    UPDATE users
    SET last_login_at = NOW(),
        auth_provider = COALESCE(NULLIF(auth_provider, ''), 'email')
    WHERE id = $1
    RETURNING *
    `,
    [user.id],
  );
  return { user: updated.rows[0], created: false };
}

async function recordCoinTransaction(client, details) {
  await client.query(
    `
    INSERT INTO economy_transactions (user_id, transaction_type, amount, balance_after, idempotency_key, metadata_json)
    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    `,
    [
      details.userId,
      details.transactionType,
      details.amount,
      details.balanceAfter,
      details.idempotencyKey || null,
      JSON.stringify(details.metadata || {}),
    ],
  );
}

async function handleFirebaseAuth(req, res) {
  if (!firebaseAuth) {
    fail(res, 500, "Firebase Admin is not configured on the server.");
    return;
  }
  const body = await parseBody(req);
  const idToken = String(body.idToken || "").trim();
  const provider = normalizeProvider(body.provider || "firebase");
  if (!idToken) {
    fail(res, 400, "Missing Firebase ID token.");
    return;
  }
  try {
    const decodedToken = await firebaseAuth.verifyIdToken(idToken);
    const { user, created } = await resolveOrCreateFirebaseUser(decodedToken, provider);
    await writeAnalyticsEvent({
      userId: user.id,
      eventName: "auth_verified",
      source: "server",
      payload: { provider, newUser: created },
    });
    json(res, 200, await buildAuthResponse(user, created));
  } catch (error) {
    fail(res, 401, error instanceof Error ? error.message : "Firebase authentication failed.");
  }
}

async function handleLegacySignup(req, res) {
  const body = await parseBody(req);
  const { displayName = "", email = "", password = "" } = body;
  if (!String(displayName).trim() || !String(email).trim() || String(password).length < 6) {
    fail(res, 400, "Display name, email, and a 6+ character password are required.");
    return;
  }
  try {
    const { user, created } = await ensureLegacyUser(displayName, email, password, true);
    json(res, 201, await buildAuthResponse(user, created));
  } catch (error) {
    fail(res, 409, error instanceof Error ? error.message : "Could not create account.");
  }
}

async function handleLegacyLogin(req, res) {
  const body = await parseBody(req);
  const { email = "", password = "" } = body;
  if (!String(email).trim() || !String(password)) {
    fail(res, 400, "Email and password are required.");
    return;
  }
  try {
    const { user, created } = await ensureLegacyUser("", email, password, false);
    json(res, 200, await buildAuthResponse(user, created));
  } catch (error) {
    fail(res, 401, error instanceof Error ? error.message : "Could not log in.");
  }
}

async function handleMe(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const profile = await fetchUserProfile(session.userId);
  if (!profile) {
    fail(res, 404, "User not found.");
    return;
  }
  json(res, 200, profile);
}

async function handleWallet(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const profile = await fetchUserProfile(session.userId);
  if (!profile) {
    fail(res, 404, "User not found.");
    return;
  }
  json(res, 200, profile.wallet);
}

async function handleAuthLogout(req, res) {
  const token = getBearerToken(req);
  if (token) {
    await revokeAppSession(token);
  }
  json(res, 200, { ok: true });
}

async function handleTrackEvent(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const body = await parseBody(req);
  const eventName = String(body.eventName || "").trim();
  const payload = body.payload && typeof body.payload === "object" ? body.payload : {};
  const matchId = body.matchId ? String(body.matchId) : null;
  if (!eventName) {
    fail(res, 400, "eventName is required.");
    return;
  }
  await writeAnalyticsEvent({
    userId: session.userId,
    eventName,
    source: "android",
    matchId,
    payload,
  });
  json(res, 200, { ok: true });
}

// NEW: status endpoint used by Android to fetch opponent names.
async function handleMatchmakingStatus(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const url = new URL(req.url, "http://localhost");
  const matchId = String(url.searchParams.get("matchId") || "").trim();
  if (!matchId) return fail(res, 400, "matchId is required.");

  const status = matchmaking.getStatus({ matchId });
  if (!status) return fail(res, 404, "Match not found.");

  const opponentDisplayNames = status.players
    .filter((p) => p.userId !== session.userId)
    .map((p) => p.displayName);

  json(res, 200, {
    matchId: status.matchId,
    playerCount: status.playerCount,
    opponentDisplayNames,
  });
}

async function handleMatchmakingJoin(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const body = await parseBody(req);
  const playerCount = Number(body.playerCount || 0);
  const entryFee = MATCH_ENTRY_FEES[playerCount];
  if (!entryFee) {
    fail(res, 400, "Unsupported player count.");
    return;
  }

  // IMPORTANT: matchId is now created by matchmaking manager (when paired / bot-filled).
  const rewardPool = entryFee * playerCount;
  const joinAttemptId = crypto.randomBytes(10).toString("hex");

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const userResult = await client.query(`SELECT id, coins FROM users WHERE id = $1 FOR UPDATE`, [session.userId]);
    if (!userResult.rowCount) throw new Error("User not found.");
    const user = userResult.rows[0];
    if (user.coins < entryFee) {
      await client.query("ROLLBACK");
      fail(res, 400, "Not enough coins to join this queue.");
      return;
    }

    const updatedCoins = user.coins - entryFee;
    await client.query(`UPDATE users SET coins = $2, rank_tier = $3 WHERE id = $1`, [
      session.userId,
      updatedCoins,
      computeRankTier(updatedCoins),
    ]);
    await recordCoinTransaction(client, {
      userId: session.userId,
      transactionType: "match_entry",
      amount: -entryFee,
      balanceAfter: updatedCoins,
      idempotencyKey: `entryAttempt:${session.userId}:${joinAttemptId}`,
      metadata: { playerCount, entryFee, rewardPool },
    });
    await client.query("COMMIT");

    matchmaking.join({
      req,
      res,
      userId: session.userId,
      displayName: session.displayName || "Player",
      playerCount,
      entryFee,
      rewardPool,
      coinBalance: updatedCoins,
      isRanked: true,
    });
    return;
  } catch (error) {
    await client.query("ROLLBACK");
    fail(res, 500, error instanceof Error ? error.message : "Could not join matchmaking.");
  } finally {
    client.release();
  }
}

async function handleDailyClaim(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      `SELECT coins, last_daily_claim_at FROM users WHERE id = $1 FOR UPDATE`,
      [session.userId],
    );
    if (!result.rowCount) throw new Error("User not found.");
    const user = result.rows[0];
    if (user.last_daily_claim_at && todayKey(user.last_daily_claim_at) === todayKey()) {
      await client.query("ROLLBACK");
      fail(res, 409, "Daily coins already claimed today.");
      return;
    }
    const updatedCoins = user.coins + DAILY_COIN_REWARD;
    await client.query(
      `UPDATE users SET coins = $2, rank_tier = $3, last_daily_claim_at = NOW() WHERE id = $1`,
      [session.userId, updatedCoins, computeRankTier(updatedCoins)],
    );
    await recordCoinTransaction(client, {
      userId: session.userId,
      transactionType: "daily_claim",
      amount: DAILY_COIN_REWARD,
      balanceAfter: updatedCoins,
      idempotencyKey: `daily:${session.userId}:${todayKey()}`,
      metadata: {},
    });
    await client.query("COMMIT");
    json(res, 200, {
      coins: updatedCoins,
      rewardCoins: DAILY_COIN_REWARD,
      rankTier: computeRankTier(updatedCoins),
    });
  } catch (error) {
    await client.query("ROLLBACK");
    fail(res, 500, error instanceof Error ? error.message : "Could not claim daily reward.");
  } finally {
    client.release();
  }
}

async function handleQuestList(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  json(res, 200, { quests: await fetchQuestState(session.userId) });
}

async function handleQuestClaim(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const body = await parseBody(req);
  const questKey = String(body.questKey || "").trim();
  const definition = QUEST_DEFINITIONS.find((quest) => quest.key === questKey);
  if (!definition) {
    fail(res, 404, "Quest not found.");
    return;
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const questResult = await client.query(
      `SELECT progress, claimed_at FROM user_quests WHERE user_id = $1 AND quest_key = $2 FOR UPDATE`,
      [session.userId, questKey],
    );
    if (!questResult.rowCount || questResult.rows[0].progress < definition.target) {
      await client.query("ROLLBACK");
      fail(res, 400, "Quest is not complete yet.");
      return;
    }
    if (questResult.rows[0].claimed_at) {
      await client.query("ROLLBACK");
      fail(res, 409, "Quest reward already claimed.");
      return;
    }
    const userResult = await client.query(`SELECT coins FROM users WHERE id = $1 FOR UPDATE`, [session.userId]);
    const updatedCoins = userResult.rows[0].coins + definition.rewardCoins;
    await client.query(`UPDATE users SET coins = $2, rank_tier = $3 WHERE id = $1`, [
      session.userId,
      updatedCoins,
      computeRankTier(updatedCoins),
    ]);
    await client.query(
      `UPDATE user_quests SET claimed_at = NOW() WHERE user_id = $1 AND quest_key = $2`,
      [session.userId, questKey],
    );
    await recordCoinTransaction(client, {
      userId: session.userId,
      transactionType: "quest_claim",
      amount: definition.rewardCoins,
      balanceAfter: updatedCoins,
      idempotencyKey: `quest:${session.userId}:${questKey}`,
      metadata: { questKey },
    });
    await client.query("COMMIT");
    json(res, 200, {
      questKey,
      rewardCoins: definition.rewardCoins,
      coinBalance: updatedCoins,
      quests: await fetchQuestState(session.userId),
    });
  } catch (error) {
    await client.query("ROLLBACK");
    fail(res, 500, error instanceof Error ? error.message : "Could not claim quest reward.");
  } finally {
    client.release();
  }
}

async function handleSettleMatch(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const body = await parseBody(req);
  const matchId = String(body.matchId || "").trim();
  const playerCount = Number(body.playerCount || 0);
  const placement = Number(body.placement || 0);
  const usedBotFill = Boolean(body.usedBotFill);
  const score = Number(body.score || 0);
  const entryFee = MATCH_ENTRY_FEES[playerCount];
  if (!matchId || !entryFee || placement <= 0) {
    fail(res, 400, "matchId, playerCount, and placement are required.");
    return;
  }
  const rewardPool = usedBotFill ? Math.floor(entryFee * playerCount * 0.7) : entryFee * playerCount;
  let rewardCoins = 0;
  if (placement === 1) rewardCoins = rewardPool;
  else if (playerCount === 4 && placement === 2) rewardCoins = Math.floor(rewardPool * 0.35);

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const existing = await client.query(
      `SELECT 1 FROM economy_transactions WHERE idempotency_key = $1`,
      [`settle:${matchId}`],
    );
    if (existing.rowCount) {
      await client.query("ROLLBACK");
      const profile = await fetchUserProfile(session.userId);
      json(res, 200, { settled: false, ...profile.wallet });
      return;
    }

    const userResult = await client.query(
      `SELECT coins, rank_tier, win_streak FROM users WHERE id = $1 FOR UPDATE`,
      [session.userId],
    );
    if (!userResult.rowCount) throw new Error("User not found.");
    const user = userResult.rows[0];
    const updatedCoins = user.coins + rewardCoins;
    const updatedStreak = placement === 1 ? (user.win_streak || 0) + 1 : 0;
    const updatedRank = computeRankTier(updatedCoins);
    await client.query(
      `UPDATE users SET coins = $2, win_streak = $3, rank_tier = $4 WHERE id = $1`,
      [session.userId, updatedCoins, updatedStreak, updatedRank],
    );
    await recordCoinTransaction(client, {
      userId: session.userId,
      transactionType: "match_settle",
      amount: rewardCoins,
      balanceAfter: updatedCoins,
      idempotencyKey: `settle:${matchId}`,
      metadata: { matchId, playerCount, placement, usedBotFill, score, rewardPool },
    });
    await client.query("COMMIT");

    await writeAnalyticsEvent({
      userId: session.userId,
      eventName: "coin_reward_granted",
      source: "server",
      matchId,
      payload: { placement, rewardCoins, coinBalance: updatedCoins, usedBotFill },
    });
    if (updatedRank !== user.rank_tier) {
      await writeAnalyticsEvent({
        userId: session.userId,
        eventName: "rank_changed",
        source: "server",
        matchId,
        payload: { previous: user.rank_tier, next: updatedRank },
      });
    }
    json(res, 200, {
      settled: true,
      coins: updatedCoins,
      rewardCoins,
      rankTier: updatedRank,
      winStreak: updatedStreak,
      rewardPool,
    });
  } catch (error) {
    await client.query("ROLLBACK");
    fail(res, 500, error instanceof Error ? error.message : "Could not settle match.");
  } finally {
    client.release();
  }
}

async function routeRequest(req, res) {
  if (req.method === "OPTIONS") {
    json(res, 204, {});
    return;
  }
  if (req.method === "GET" && req.url === "/health") {
    json(res, 200, {
      ok: true,
      firebaseConfigured: Boolean(firebaseAuth),
      databaseConfigured: Boolean(DATABASE_URL),
    });
    return;
  }
  if (req.method === "POST" && req.url === "/auth/signup") return handleLegacySignup(req, res);
  if (req.method === "POST" && req.url === "/auth/login") return handleLegacyLogin(req, res);
  if (req.method === "POST" && req.url === "/auth/firebase") return handleFirebaseAuth(req, res);
  if (req.method === "POST" && req.url === "/auth/logout") return handleAuthLogout(req, res);
  if (req.method === "GET" && req.url === "/me") return handleMe(req, res);
  if (req.method === "GET" && req.url === "/wallet") return handleWallet(req, res);
  if (req.method === "POST" && req.url === "/events") return handleTrackEvent(req, res);
  if (req.method === "POST" && req.url === "/matchmaking/join") return handleMatchmakingJoin(req, res);
  if (req.method === "GET" && req.url.startsWith("/matchmaking/status")) return handleMatchmakingStatus(req, res);
  if (req.method === "POST" && req.url === "/economy/claim-daily") return handleDailyClaim(req, res);
  if (req.method === "POST" && req.url === "/economy/settle-match") return handleSettleMatch(req, res);
  if (req.method === "GET" && req.url === "/quests") return handleQuestList(req, res);
  if (req.method === "POST" && req.url === "/quests/claim") return handleQuestClaim(req, res);
  fail(res, 404, "Not found.");
}

const server = http.createServer(async (req, res) => {
  try {
    await routeRequest(req, res);
  } catch (error) {
    console.error("Unexpected server error", error);
    fail(res, 500, error instanceof Error ? error.message : "Unexpected server error.");
  }
});

const wss = new WebSocketServer({ server });

function id() {
  return Math.random().toString(36).slice(2, 8).toUpperCase();
}

function buildDeck() {
  const deck = [];
  for (const suit of SUITS) {
    for (const [rank, value] of RANKS) {
      deck.push({ rank, value, suit, id: `${rank}${suit}` });
    }
  }
  for (let i = deck.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [deck[i], deck[j]] = [deck[j], deck[i]];
  }
  return deck;
}

function createPlayer(name, index) {
  return {
    id: index,
    name,
    hand: [],
    groups: [[], [], []],
    discarded: null,
    ready: false,
  };
}

function createRoom(hostName) {
  const roomId = id();
  const room = {
    id: roomId,
    phase: "lobby",
    round: 1,
    totalRounds: 3,
    activeArrangePlayerIndex: 0,
    battleGroupIndex: 0,
    battleRevealCount: 0,
    scores: [0],
    groupsWon: [0],
    players: [createPlayer(hostName, 0)],
  };
  rooms.set(roomId, room);
  return room;
}

function dealRound(room) {
  const deck = buildDeck();
  room.players = room.players.map((player, index) => ({
    ...player,
    id: index,
    hand: deck.splice(0, 10),
    groups: [[], [], []],
    discarded: null,
    ready: false,
  }));
  room.phase = "arrange";
  room.activeArrangePlayerIndex = 0;
  room.battleGroupIndex = 0;
  room.battleRevealCount = 0;
}

function evaluateGroup(cards) {
  if (cards.length !== 3) return { type: "high", score: 0, strength: 1 };
  const values = cards.map((c) => c.value).sort((a, b) => a - b);
  const allSameSuit = cards.every((card) => card.suit === cards[0].suit);
  const allSameRank = cards.every((card) => card.value === cards[0].value);
  const isSequence =
    (values[1] === values[0] + 1 && values[2] === values[1] + 1) ||
    (values[0] === 12 && values[1] === 13 && values[2] === 14) ||
    (values[0] === 2 && values[1] === 3 && values[2] === 14);
  const sequenceHigh = values[0] === 2 && values[1] === 3 && values[2] === 14 ? 3 : values[2];

  if (allSameRank) return { type: "toak", score: values[0], strength: 6 };
  if (isSequence && allSameSuit) return { type: "pure", score: sequenceHigh, strength: 5 };
  if (allSameSuit) return { type: "flush", score: values[2] * 10000 + values[1] * 100 + values[0], strength: 4 };
  if (isSequence) return { type: "impure", score: sequenceHigh, strength: 3 };
  const counts = {};
  values.forEach((value) => {
    counts[value] = (counts[value] || 0) + 1;
  });
  const pairRank = Number(Object.keys(counts).find((key) => counts[key] === 2));
  if (pairRank) {
    const kicker = values.slice().reverse().find((value) => value !== pairRank) || 0;
    return { type: "pair", score: pairRank * 100 + kicker, strength: 2 };
  }
  const desc = values.slice().reverse();
  return { type: "high", score: desc[0] * 10000 + desc[1] * 100 + desc[2], strength: 1 };
}

function compareGroups(a, b) {
  const left = evaluateGroup(a);
  const right = evaluateGroup(b);
  if (left.strength !== right.strength) return left.strength - right.strength;
  return left.score - right.score;
}

function resolveBattle(room) {
  const showOrder = room.players.map((_, index) => (index + 1) % room.players.length);
  let winner = showOrder[0];
  for (const challenger of showOrder.slice(1)) {
    const compareResult = compareGroups(
      room.players[challenger].groups[room.battleGroupIndex],
      room.players[winner].groups[room.battleGroupIndex],
    );
    if (compareResult >= 0) {
      winner = challenger;
    }
  }
  return winner;
}

function nextUnreadyPlayer(room) {
  const next = room.players.findIndex((player) => !player.ready);
  return next === -1 ? 0 : next;
}

function sanitizeRoomState(room, playerIndex) {
  return {
    id: room.id,
    phase: room.phase,
    round: room.round,
    totalRounds: room.totalRounds,
    localPlayerIndex: playerIndex,
    activeArrangePlayerIndex: room.activeArrangePlayerIndex,
    battleGroupIndex: room.battleGroupIndex,
    battleRevealCount: room.battleRevealCount,
    scores: room.scores,
    groupsWon: room.groupsWon,
    players: room.players.map((player, index) => ({
      id: player.id,
      name: player.name,
      ready: player.ready,
      discarded: index === playerIndex ? player.discarded : null,
      hand: index === playerIndex ? player.hand : [],
      groups: index === playerIndex || room.phase === "battle" || room.phase === "results"
        ? player.groups
        : [[], [], []],
    })),
  };
}

function broadcastRoom(room) {
  clientMeta.forEach((meta, socket) => {
    if (meta.roomId !== room.id || socket.readyState !== 1) return;
    send(socket, "state", sanitizeRoomState(room, meta.playerIndex));
  });
}

function send(socket, type, payload) {
  socket.send(JSON.stringify({ type, payload }));
}

function socketFail(socket, message) {
  send(socket, "error", { message });
}

function roomFor(socket) {
  const meta = clientMeta.get(socket);
  return meta ? rooms.get(meta.roomId) : null;
}

function requireRoom(socket) {
  const room = roomFor(socket);
  if (!room) socketFail(socket, "Room not found.");
  return room;
}

function requirePlayer(socket, room) {
  const meta = clientMeta.get(socket);
  if (!meta) {
    socketFail(socket, "Player not registered.");
    return null;
  }
  return room.players[meta.playerIndex];
}

wss.on("connection", (socket) => {
  socket.on("message", (raw) => {
    void (async () => {
      let message;
      try {
        message = JSON.parse(String(raw));
      } catch {
        socketFail(socket, "Invalid JSON.");
        return;
      }

      const { type, payload = {} } = message;

      if (type === "matchmaking_listen") {
        const token = String(payload.authToken || payload.token || "").trim();
        if (!token) return socketFail(socket, "Missing auth token.");
        const session = await fetchUserByToken(token);
        if (!session) return socketFail(socket, "Invalid or expired auth token.");
        matchmakingSockets.set(session.userId, socket);
        send(socket, "matchmaking_listening", { ok: true });
        return;
      }

      if (type === "join_match") {
        const matchId = String(payload.matchId || "").trim().toUpperCase();
        const token = String(payload.authToken || payload.token || "").trim();
        if (!matchId) return socketFail(socket, "Missing matchId.");
        if (!token) return socketFail(socket, "Missing auth token.");
        const session = await fetchUserByToken(token);
        if (!session) return socketFail(socket, "Invalid or expired auth token.");
        attachSocketToMatchRoom({ socket, matchId, userId: session.userId });
        return;
      }

    if (type === "create_room") {
      const room = createRoom(payload.name || "Host");
      clientMeta.set(socket, { roomId: room.id, playerIndex: 0 });
      send(socket, "room_created", { roomId: room.id, playerIndex: 0 });
      broadcastRoom(room);
      return;
    }

    if (type === "join_room") {
      const room = rooms.get(String(payload.roomId || "").toUpperCase());
      if (!room) return socketFail(socket, "Room not found.");
      if (room.players.length >= 4) return socketFail(socket, "Room is full.");
      const playerIndex = room.players.length;
      room.players.push(createPlayer(payload.name || `Player ${playerIndex + 1}`, playerIndex));
      room.scores = room.players.map((_, index) => room.scores[index] || 0);
      room.groupsWon = room.players.map((_, index) => room.groupsWon[index] || 0);
      clientMeta.set(socket, { roomId: room.id, playerIndex });
      broadcastRoom(room);
      return;
    }

    const room = requireRoom(socket);
    if (!room) return;
    const player = requirePlayer(socket, room);
    if (!player) return;
    const meta = clientMeta.get(socket);

    if (type === "start_game") {
      if (meta.playerIndex !== 0) return socketFail(socket, "Only the host can start the game.");
      if (room.players.length < 2) return socketFail(socket, "Need at least 2 players.");
      room.scores = room.players.map(() => 0);
      room.groupsWon = room.players.map(() => 0);
      room.round = 1;
      dealRound(room);
      broadcastRoom(room);
      return;
    }

    if (type === "move_card_to_group") {
      if (room.phase !== "arrange") return socketFail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return socketFail(socket, "Not your turn.");
      const { cardIndex, groupIndex } = payload;
      if (!Number.isInteger(cardIndex) || !Number.isInteger(groupIndex)) return socketFail(socket, "Invalid move.");
      if (groupIndex < 0 || groupIndex > 2) return socketFail(socket, "Invalid group.");
      if (!player.hand[cardIndex]) return socketFail(socket, "Card not found.");
      if (player.groups[groupIndex].length >= 3) return socketFail(socket, "Group is full.");
      const [card] = player.hand.splice(cardIndex, 1);
      player.groups[groupIndex].push(card);
      broadcastRoom(room);
      return;
    }

    if (type === "discard_card") {
      if (room.phase !== "arrange") return socketFail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return socketFail(socket, "Not your turn.");
      const { cardIndex } = payload;
      if (player.discarded) return socketFail(socket, "Card already discarded.");
      if (player.groups.some((group) => group.length !== 3)) return socketFail(socket, "Finish all groups first.");
      const [card] = player.hand.splice(cardIndex, 1);
      if (!card) return socketFail(socket, "Card not found.");
      player.discarded = card;
      broadcastRoom(room);
      return;
    }

    if (type === "confirm_ready") {
      if (room.phase !== "arrange") return socketFail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return socketFail(socket, "Not your turn.");
      if (player.groups.some((group) => group.length !== 3) || !player.discarded) {
        return socketFail(socket, "Groups or discard incomplete.");
      }
      player.groups = player.groups.slice().sort((a, b) => compareGroups(b, a));
      player.ready = true;
      if (room.players.every((entry) => entry.ready)) {
        room.phase = "battle";
        room.activeArrangePlayerIndex = 0;
        room.battleGroupIndex = 0;
        room.battleRevealCount = 0;
      } else {
        room.activeArrangePlayerIndex = nextUnreadyPlayer(room);
      }
      broadcastRoom(room);
      return;
    }

    if (type === "reveal_next") {
      if (room.phase !== "battle") return socketFail(socket, "Not in battle phase.");
      room.battleRevealCount += 1;
      broadcastRoom(room);
      return;
    }

    if (type === "score_group") {
      if (room.phase !== "battle") return socketFail(socket, "Not in battle phase.");
      const winner = resolveBattle(room);
      room.groupsWon[winner] += 1;
      room.battleGroupIndex += 1;
      room.battleRevealCount = 0;

      if (room.battleGroupIndex >= 3) {
        room.groupsWon.forEach((wins, index) => {
          room.scores[index] += wins;
        });
        if (room.round >= room.totalRounds) {
          room.phase = "results";
        } else {
          room.round += 1;
          dealRound(room);
        }
      }

      broadcastRoom(room);
    }
    })().catch((error) => {
      socketFail(socket, error instanceof Error ? error.message : "Unexpected server error.");
    });
  });

  socket.on("close", () => {
    // If this socket was registered for matchmaking, remove it.
    matchmakingSockets.forEach((value, key) => {
      if (value === socket) matchmakingSockets.delete(key);
    });

    const meta = clientMeta.get(socket);
    clientMeta.delete(socket);
    if (!meta) return;
    const room = rooms.get(meta.roomId);
    if (!room) return;
    room.players = room.players.filter((_, index) => index !== meta.playerIndex);
    if (room.players.length === 0) {
      rooms.delete(meta.roomId);
      return;
    }
    room.players = room.players.map((player, index) => ({ ...player, id: index }));
    room.scores = room.players.map((_, index) => room.scores[index] || 0);
    room.groupsWon = room.players.map((_, index) => room.groupsWon[index] || 0);
    room.activeArrangePlayerIndex = Math.min(room.activeArrangePlayerIndex, room.players.length - 1);
    broadcastRoom(room);
  });
});

ensureSchema()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Trio Clash auth, economy, analytics, and multiplayer server running on :${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Failed to initialize database schema.", error);
    process.exit(1);
  });
