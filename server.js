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

// A matchmaking match must keep progressing even when a player's device is
// frozen (screen lock / Doze) or their network drops — those clients stop
// sending confirm_ready / reveal_next and, before these, the whole room hung
// for every player until it was force-quit.
//
// - ARRANGE_TIMEOUT_MS: server auto-arranges any player who hasn't confirmed
//   and moves the room to battle. Longer than the client's 60 s arrange timer
//   so a healthy client's own auto_ready always wins the race.
// - BATTLE_STEP_TIMEOUT_MS: server auto-advances the showdown (reveal, then
//   score) so an absent player can't stall it. Any connected player can still
//   drive it faster by tapping.
// - MATCH_ABANDON_MS: once NOBODY is connected to a room, it's retired after
//   this grace period (covers a brief double-drop, e.g. a server blip).
const ARRANGE_TIMEOUT_MS = Number(process.env.ARRANGE_TIMEOUT_MS || 75000);
const BATTLE_STEP_TIMEOUT_MS = Number(process.env.BATTLE_STEP_TIMEOUT_MS || 25000);
const MATCH_ABANDON_MS = Number(process.env.MATCH_ABANDON_MS || 120000);
const WS_HEARTBEAT_MS = Number(process.env.WS_HEARTBEAT_MS || 30000);

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
  const cfg = modeConfig(status.mode);
  const room = {
    id: normalized,
    phase: "lobby",
    round: 1,
    totalRounds: 3,
    mode: status.mode || "ten",
    groupCount: cfg.groupCount,
    cardsPerHand: cfg.cardsPerHand,
    hasDiscard: cfg.hasDiscard,
    activeArrangePlayerIndex: 0,
    battleGroupIndex: 0,
    battleRevealCount: 0,
    battleWinnerIndex: null,
    scores: status.players.map(() => 0),
    groupsWon: status.players.map(() => 0),
    players: status.players.map((p, index) => ({
      // userId pins the seat to a person, so a reconnecting socket rebinds to
      // the same hand/groups/score instead of being dropped and re-indexed.
      // `connected` tracks live socket presence for the abandon grace timer.
      ...createPlayer(p.displayName || `Player ${index + 1}`, index, cfg.groupCount),
      userId: p.userId,
      connected: false,
    })),
    // Per-room timer handles (arrange auto-advance, battle step auto-advance,
    // abandon grace). Never serialised to clients — sanitizeRoomState rebuilds
    // the payload field by field.
    timers: { arrange: null, battle: null, abandon: null },
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

  // Reconnect handling: evict any earlier socket still bound to this seat. Its
  // eventual 'close' would otherwise tear down the seat we're rebinding (and it
  // would keep receiving broadcasts for a player who has already moved on).
  clientMeta.forEach((otherMeta, otherSocket) => {
    if (otherSocket === socket) return;
    if (otherMeta.roomId === room.id && otherMeta.playerIndex === playerIndex) {
      clientMeta.delete(otherSocket);
      try { otherSocket.close(4000, "Replaced by a newer connection"); } catch {}
    }
  });

  clientMeta.set(socket, { roomId: room.id, playerIndex, userId });
  if (room.players[playerIndex]) room.players[playerIndex].connected = true;
  clearRoomTimer(room, "abandon");
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

function isMatchmakingRoomId(roomId) {
  const normalized = String(roomId || "").trim().toUpperCase();
  if (!normalized) return false;
  return Boolean(matchmaking.getStatus({ matchId: normalized }));
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
  let email = String(decodedToken.email || "").trim().toLowerCase();
  const photoUrl = decodedToken.picture || null;
  const authProvider = normalizeProvider(
    providerHint || decodedToken.firebase?.sign_in_provider || decodedToken.sign_in_provider || "firebase",
  );

  if (!email) {
    // The ID token's own "email" claim only carries an account's top-level
    // primary email, which Firebase sets only when it considers the linked
    // provider's email verified. Facebook's Graph response never includes a
    // verified flag, so this can be blank even though Firebase recorded the
    // address against the linked Facebook provider. Ask Admin SDK for the
    // authoritative user record and fall back to that provider-linked email
    // — never trust a client-supplied email here, since an authenticated
    // caller could otherwise claim any address and get linked into another
    // player's account by the email-match lookup below.
    try {
      const userRecord = await firebaseAuth.getUser(firebaseUid);
      const providerEmail = userRecord.providerData.find((p) => p.email)?.email;
      if (providerEmail) email = String(providerEmail).trim().toLowerCase();
    } catch (_) {
      // Falls through to the missing-email error below.
    }
  }

  if (!email) {
    throw new Error("Firebase account is missing an email address.");
  }

  const displayName = String(decodedToken.name || email || "Player").trim();

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
    mode: status.mode || "ten",
    opponentDisplayNames,
  });
}

async function handleMatchmakingJoin(req, res) {
  const session = await requireAuth(req, res);
  if (!session) return;
  const body = await parseBody(req);
  const playerCount = Number(body.playerCount || 0);
  const mode = body.mode === "fifteen" ? "fifteen" : "ten";
  const entryFee = MATCH_ENTRY_FEES[playerCount];
  if (!entryFee) {
    fail(res, 400, "Unsupported player count.");
    return;
  }

  // matchId is created by the matchmaking manager (when paired / bot-filled).
  const rewardPool = entryFee * playerCount;

  // Matchmaking is FREE. The coin economy is headless — settled server-side on
  // match completion and never surfaced in the app — so joining a queue is no
  // longer gated on a coin balance, nor charged an entry fee. coinBalance below
  // is informational only.
  let coinBalance = 0;
  try {
    const userResult = await db.query(`SELECT coins FROM users WHERE id = $1`, [session.userId]);
    coinBalance = userResult.rows[0]?.coins ?? 0;
  } catch {
    // Non-fatal: the balance is not required to join.
  }

  matchmaking.join({
    req,
    res,
    userId: session.userId,
    displayName: session.displayName || "Player",
    playerCount,
    mode,
    entryFee,
    rewardPool,
    coinBalance,
    isRanked: true,
  });
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

// "ten" is the original 10-card / 3-group game. "fifteen" is the Big Hand
// variant: 15 cards, 5 groups of 3, no discard.
function modeConfig(mode) {
  return mode === "fifteen"
    ? { cardsPerHand: 15, groupCount: 5, hasDiscard: false }
    : { cardsPerHand: 10, groupCount: 3, hasDiscard: true };
}

function emptyGroups(count) {
  return Array.from({ length: count }, () => []);
}

function buildDeck(deckCount = 1) {
  const copies = Math.max(1, deckCount);
  const deck = [];
  for (let copy = 0; copy < copies; copy += 1) {
    for (const suit of SUITS) {
      for (const [rank, value] of RANKS) {
        deck.push({
          rank,
          value,
          suit,
          // Suffix the id only when a second deck is in play (15-card 4-player),
          // so the two identical cards stay distinct on the client.
          id: copies > 1 ? `${rank}${suit}#${copy}` : `${rank}${suit}`,
          copyIndex: copy,
        });
      }
    }
  }
  for (let i = deck.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [deck[i], deck[j]] = [deck[j], deck[i]];
  }
  return deck;
}

function createPlayer(name, index, groupCount = 3) {
  return {
    id: index,
    name,
    hand: [],
    groups: emptyGroups(groupCount),
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
    mode: "ten",
    groupCount: 3,
    cardsPerHand: 10,
    hasDiscard: true,
    activeArrangePlayerIndex: 0,
    battleGroupIndex: 0,
    battleRevealCount: 0,
    battleWinnerIndex: null,
    scores: [0],
    groupsWon: [0],
    players: [createPlayer(hostName, 0)],
  };
  rooms.set(roomId, room);
  return room;
}

function dealRound(room) {
  const groupCount = room.groupCount || 3;
  const cardsPerHand = room.cardsPerHand || 10;
  const needed = cardsPerHand * room.players.length;
  const deckCount = Math.max(1, Math.ceil(needed / 52));
  const deck = buildDeck(deckCount);
  room.players = room.players.map((player, index) => ({
    ...player,
    id: index,
    hand: deck.splice(0, cardsPerHand),
    groups: emptyGroups(groupCount),
    discarded: null,
    ready: false,
  }));
  room.phase = "arrange";
  room.activeArrangePlayerIndex = 0;
  room.battleGroupIndex = 0;
  room.battleRevealCount = 0;
  room.battleWinnerIndex = null;
  // A fresh arrange phase: no battle step pending, and the server owns a
  // fallback deadline in case a client never sends confirm_ready / auto_ready.
  clearRoomTimer(room, "battle");
  scheduleArrangeTimeout(room);
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
  const groupCount = room.groupCount || 3;
  return {
    id: room.id,
    phase: room.phase,
    round: room.round,
    totalRounds: room.totalRounds,
    mode: room.mode || "ten",
    groupCount,
    localPlayerIndex: playerIndex,
    activeArrangePlayerIndex: room.activeArrangePlayerIndex,
    battleGroupIndex: room.battleGroupIndex,
    battleRevealCount: room.battleRevealCount,
    // Send the winner of the current group once all cards are revealed,
    // so the client can highlight the winner and enable scoring.
    battleWinnerIndex: room.battleWinnerIndex ?? null,
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
        : emptyGroups(groupCount),
    })),
  };
}

function broadcastRoom(room) {
  clientMeta.forEach((meta, socket) => {
    if (meta.roomId !== room.id || socket.readyState !== 1) return;
    send(socket, "state", sanitizeRoomState(room, meta.playerIndex));
  });
}

// ---------------------------------------------------------------------------
// Keep-the-match-moving timers.
//
// Every one of these does exactly what a client tap would have done — they are
// a fallback for when the tap never arrives (frozen device, lost network), not
// a separate code path. Any still-connected player can always drive the match
// faster by acting normally.
// ---------------------------------------------------------------------------

function roomTimers(room) {
  if (!room.timers) room.timers = { arrange: null, battle: null, abandon: null };
  return room.timers;
}

function clearRoomTimer(room, key) {
  const timers = room && room.timers;
  if (timers && timers[key]) {
    clearTimeout(timers[key]);
    timers[key] = null;
  }
}

function clearAllRoomTimers(room) {
  clearRoomTimer(room, "arrange");
  clearRoomTimer(room, "battle");
  clearRoomTimer(room, "abandon");
}

// Auto-arrange one player's hand and mark them ready. Mirrors the "auto_ready"
// message handler (same simple sort-and-chunk the client uses on timeout).
function autoArrangePlayer(room, player) {
  const groupCount = room.groupCount || 3;
  const needDiscard = room.hasDiscard !== false;
  const allCards = [...player.hand, ...player.groups.flat()];
  if (player.discarded) allCards.push(player.discarded);
  const sorted = [...allCards].sort((a, b) => b.value - a.value);
  player.discarded = needDiscard ? sorted[sorted.length - 1] : null;
  const remaining = sorted.slice(0, groupCount * 3);
  player.groups = Array.from({ length: groupCount }, (_, g) => remaining.slice(g * 3, g * 3 + 3));
  player.groups = player.groups.slice().sort((a, b) => compareGroups(b, a));
  player.hand = [];
  player.ready = true;
}

function enterBattle(room) {
  room.phase = "battle";
  room.battleGroupIndex = 0;
  room.battleRevealCount = 0;
  room.battleWinnerIndex = null;
  clearRoomTimer(room, "arrange");
  scheduleBattleTimeout(room);
}

function scheduleArrangeTimeout(room) {
  const timers = roomTimers(room);
  clearRoomTimer(room, "arrange");
  timers.arrange = setTimeout(() => {
    if (!rooms.has(room.id) || room.phase !== "arrange") return;
    let changed = false;
    for (const player of room.players) {
      if (!player.ready) {
        autoArrangePlayer(room, player);
        changed = true;
      }
    }
    if (room.players.every((p) => p.ready)) {
      enterBattle(room);
    }
    if (changed) broadcastRoom(room);
  }, ARRANGE_TIMEOUT_MS);
}

function scheduleBattleTimeout(room) {
  const timers = roomTimers(room);
  clearRoomTimer(room, "battle");
  timers.battle = setTimeout(() => {
    if (!rooms.has(room.id) || room.phase !== "battle") return;
    if (room.battleWinnerIndex == null) revealCurrentGroup(room);
    else scoreCurrentGroup(room);
  }, BATTLE_STEP_TIMEOUT_MS);
}

// Flip the current battle group face-up for everyone and resolve its winner.
// Same effect as any player sending "reveal_next".
function revealCurrentGroup(room) {
  if (room.phase !== "battle") return;
  room.battleRevealCount = room.players.length;
  room.battleWinnerIndex = resolveBattle(room);
  broadcastRoom(room);
  scheduleBattleTimeout(room); // then auto-advance to scoring
}

// Award the current group to its winner and advance to the next group / round /
// results. Same effect as any player sending "score_group".
function scoreCurrentGroup(room) {
  if (room.phase !== "battle" || room.battleWinnerIndex == null) return;
  const winner = room.battleWinnerIndex;
  room.groupsWon[winner] += 1;
  room.battleGroupIndex += 1;
  room.battleRevealCount = 0;
  room.battleWinnerIndex = null;

  if (room.battleGroupIndex >= (room.groupCount || 3)) {
    const maxWins = Math.max(...room.groupsWon);
    const roundWinners = room.groupsWon
      .map((wins, index) => ({ wins, index }))
      .filter((entry) => entry.wins === maxWins)
      .map((entry) => entry.index);
    const roundWinnerIndex = roundWinners.length === 1 ? roundWinners[0] : null;
    if (roundWinnerIndex != null) {
      room.scores[roundWinnerIndex] = (room.scores[roundWinnerIndex] || 0) + 1;
    }
    if (room.round >= room.totalRounds) {
      room.phase = "results";
      clearRoomTimer(room, "battle");
      clearRoomTimer(room, "arrange");
    } else {
      room.round += 1;
      room.groupsWon = room.players.map(() => 0);
      dealRound(room); // re-arms the arrange timeout, clears the battle timer
    }
  } else {
    scheduleBattleTimeout(room); // next group's reveal
  }

  broadcastRoom(room);
}

function anySocketBoundToSeat(roomId, playerIndex) {
  let found = false;
  clientMeta.forEach((meta, socket) => {
    if (meta.roomId === roomId && meta.playerIndex === playerIndex && socket.readyState === 1) {
      found = true;
    }
  });
  return found;
}

// Nobody is connected to this room. Give it a grace window (a brief double-drop
// or a server blip can knock everyone off at once) and then retire it so the
// in-memory maps and timers don't leak.
function armRoomAbandon(room) {
  const timers = roomTimers(room);
  clearRoomTimer(room, "abandon");
  timers.abandon = setTimeout(() => {
    if (!rooms.has(room.id) || connectedCount(room.id) > 0) return;
    retireRoom(room);
  }, MATCH_ABANDON_MS);
}

function retireRoom(room) {
  clearAllRoomTimers(room);
  rooms.delete(room.id);
  clientMeta.forEach((meta, socket) => {
    if (meta.roomId === room.id) clientMeta.delete(socket);
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
  socket.isAlive = true;
  socket.on("pong", markSocketAlive);

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
      const roomId = String(payload.roomId || "").trim().toUpperCase();
      if (!roomId) return socketFail(socket, "Missing roomId.");

      // Back-compat / safety: if the client is trying to join a matchmaking-created
      // room via join_room, treat it as join_match instead of adding a new seat.
      // Adding seats to a matchmaking room corrupts player indices and scoring.
      if (isMatchmakingRoomId(roomId)) {
        const token = String(payload.authToken || payload.token || "").trim();
        if (!token) return socketFail(socket, "Missing auth token.");
        const session = await fetchUserByToken(token);
        if (!session) return socketFail(socket, "Invalid or expired auth token.");
        attachSocketToMatchRoom({ socket, matchId: roomId, userId: session.userId });
        return;
      }

      const room = rooms.get(roomId);
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
      // Simultaneous arrange: each player acts on their own hand independently.
      // No turn gating here.
      const { cardIndex, groupIndex } = payload;
      if (!Number.isInteger(cardIndex) || !Number.isInteger(groupIndex)) return socketFail(socket, "Invalid move.");
      if (groupIndex < 0 || groupIndex > (room.groupCount || 3) - 1) return socketFail(socket, "Invalid group.");
      if (!player.hand[cardIndex]) return socketFail(socket, "Card not found.");
      if (player.groups[groupIndex].length >= 3) return socketFail(socket, "Group is full.");
      const [card] = player.hand.splice(cardIndex, 1);
      player.groups[groupIndex].push(card);
      broadcastRoom(room);
      return;
    }

    if (type === "discard_card") {
      if (room.phase !== "arrange") return socketFail(socket, "Not in arrange phase.");
      if (room.hasDiscard === false) return socketFail(socket, "This mode has no discard.");
      // Simultaneous arrange: no turn gating.
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
      if (player.ready) return; // Idempotent — ignore duplicate confirms.

      // The client sends the complete final arrangement in the payload.
      // This is the authoritative card state — we use it directly rather than
      // relying on individual move_card_to_group messages, which are not sent
      // anymore. This guarantees client and server have identical groups in battle.
      const { groups: groupsPayload, discarded: discardedPayload } = payload;
      const groupCount = room.groupCount || 3;
      const needDiscard = room.hasDiscard !== false;

      if (Array.isArray(groupsPayload)) {
        // Validate: groupCount groups of 3 cards each, plus a discard for "ten".
        if (
          groupsPayload.length !== groupCount ||
          groupsPayload.some((g) => !Array.isArray(g) || g.length !== 3)
        ) {
          return socketFail(socket, `Invalid arrangement: need ${groupCount} groups of 3 cards.`);
        }
        if (needDiscard && !discardedPayload) {
          return socketFail(socket, "Invalid arrangement: missing discard.");
        }
        player.groups = groupsPayload;
        player.discarded = needDiscard ? discardedPayload : null;
        player.hand = [];
      } else {
        // Fallback: validate what the server already tracked.
        if (
          player.groups.some((group) => group.length !== 3) ||
          (needDiscard && !player.discarded)
        ) {
          return socketFail(socket, "Groups or discard incomplete.");
        }
      }

      // Sort groups strongest-first (mirrors what the client does).
      player.groups = player.groups.slice().sort((a, b) => compareGroups(b, a));
      player.ready = true;

      // Broadcast immediately so all clients see this player is ready.
      broadcastRoom(room);

      // Transition to battle only once every player has confirmed.
      if (room.players.every((entry) => entry.ready)) {
        enterBattle(room);
        broadcastRoom(room);
      }
      return;
    }

    // Sent by the client when the arrange timer expires and the player hasn't
    // manually confirmed. Server auto-arranges their hand and marks them ready.
    if (type === "auto_ready") {
      if (room.phase !== "arrange") return;
      if (player.ready) return; // Already ready, nothing to do.
      autoArrangePlayer(room, player);
      broadcastRoom(room);
      if (room.players.every((entry) => entry.ready)) {
        enterBattle(room);
        broadcastRoom(room);
      }
      return;
    }

    if (type === "reveal_next") {
      if (room.phase !== "battle") return socketFail(socket, "Not in battle phase.");
      // A single reveal shows this group's cards for all players (matches the
      // bot/local flow). Server timeout does the same if nobody taps.
      revealCurrentGroup(room);
      return;
    }

    if (type === "score_group") {
      if (room.phase !== "battle") return socketFail(socket, "Not in battle phase.");
      if (room.battleWinnerIndex == null) return socketFail(socket, "Reveal all cards first.");
      scoreCurrentGroup(room);
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

    // Matchmaking rooms: seats are fixed for the life of the match. A socket
    // drop (screen lock, Wi-Fi blip, our own "replaced by newer connection"
    // eviction) must NOT delete the seat or re-index the table — the player's
    // hand, groups and score have to survive a reconnect, and the phase
    // timeouts keep the match moving while they're away. If this was a genuine
    // reconnect, a newer socket is already bound to the seat, so leave it be.
    if (isMatchmakingRoomId(room.id)) {
      if (anySocketBoundToSeat(room.id, meta.playerIndex)) return;
      const seat = room.players[meta.playerIndex];
      if (seat) seat.connected = false;
      if (connectedCount(room.id) === 0) {
        armRoomAbandon(room);
      }
      broadcastRoom(room);
      return;
    }

    // Custom lobby rooms (create_room / join_room): original behaviour — remove
    // the seat and compact the table.
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

// WebSocket heartbeat: the `ws` server does not detect a peer that vanished
// without a TCP FIN (exactly what a frozen / Doze'd phone does) — the socket
// would sit "open" for minutes. Ping every client on an interval and terminate
// any that missed the previous round-trip, so the close handler above (and its
// abandon / phase timeouts) run on a predictable ~1 minute worst case.
function markSocketAlive() {
  this.isAlive = true;
}

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((socket) => {
    if (socket.isAlive === false) {
      try { socket.terminate(); } catch {}
      return;
    }
    socket.isAlive = false;
    try { socket.ping(); } catch {}
  });
}, WS_HEARTBEAT_MS);

wss.on("close", () => clearInterval(heartbeatInterval));

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
