import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";
import pg from "pg";
import bcrypt from "bcryptjs";

const { Pool } = pg;

const PORT = Number(process.env.PORT || 8080);
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Auth endpoints will fail until it is configured.");
}

const db = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL ? { rejectUnauthorized: false } : false
});

const sessions = new Map();
const rooms = new Map();
const clientMeta = new Map();

const SUITS = ["\u2660", "\u2665", "\u2666", "\u2663"];
const RANKS = [
  ["A", 14], ["2", 2], ["3", 3], ["4", 4], ["5", 5], ["6", 6], ["7", 7],
  ["8", 8], ["9", 9], ["10", 10], ["J", 11], ["Q", 12], ["K", 13]
];

async function ensureSchema() {
  if (!DATABASE_URL) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      display_name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function json(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization"
  });
  res.end(JSON.stringify(payload));
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

function issueSession(user) {
  const token = crypto.randomBytes(24).toString("hex");
  sessions.set(token, {
    userId: user.id,
    displayName: user.display_name,
    email: user.email
  });
  return token;
}

async function handleSignup(req, res) {
  if (!DATABASE_URL) {
    json(res, 500, { error: "DATABASE_URL is not configured on the server." });
    return;
  }

  const { displayName = "", email = "", password = "" } = await parseBody(req);
  const normalizedEmail = String(email).trim().toLowerCase();
  const normalizedName = String(displayName).trim();

  if (!normalizedName || !normalizedEmail || String(password).length < 6) {
    json(res, 400, { error: "Display name, email, and a 6+ character password are required." });
    return;
  }

  const existing = await db.query("SELECT id FROM users WHERE email = $1", [normalizedEmail]);
  if (existing.rowCount) {
    json(res, 409, { error: "An account already exists for that email." });
    return;
  }

  const passwordHash = await bcrypt.hash(String(password), 10);
  const result = await db.query(
    `INSERT INTO users (display_name, email, password_hash)
     VALUES ($1, $2, $3)
     RETURNING id, display_name, email`,
    [normalizedName, normalizedEmail, passwordHash]
  );

  const user = result.rows[0];
  const token = issueSession(user);
  json(res, 201, {
    token,
    user: {
      id: user.id,
      displayName: user.display_name,
      email: user.email
    }
  });
}

async function handleLogin(req, res) {
  if (!DATABASE_URL) {
    json(res, 500, { error: "DATABASE_URL is not configured on the server." });
    return;
  }

  const { email = "", password = "" } = await parseBody(req);
  const normalizedEmail = String(email).trim().toLowerCase();
  const result = await db.query(
    "SELECT id, display_name, email, password_hash FROM users WHERE email = $1",
    [normalizedEmail]
  );

  if (!result.rowCount) {
    json(res, 401, { error: "Invalid email or password." });
    return;
  }

  const user = result.rows[0];
  const validPassword = await bcrypt.compare(String(password), user.password_hash);
  if (!validPassword) {
    json(res, 401, { error: "Invalid email or password." });
    return;
  }

  const token = issueSession(user);
  json(res, 200, {
    token,
    user: {
      id: user.id,
      displayName: user.display_name,
      email: user.email
    }
  });
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === "OPTIONS") {
      json(res, 204, {});
      return;
    }

    if (req.method === "GET" && req.url === "/health") {
      json(res, 200, { ok: true });
      return;
    }

    if (req.method === "POST" && req.url === "/auth/signup") {
      await handleSignup(req, res);
      return;
    }

    if (req.method === "POST" && req.url === "/auth/login") {
      await handleLogin(req, res);
      return;
    }

    json(res, 404, { error: "Not found." });
  } catch (error) {
    json(res, 500, { error: error instanceof Error ? error.message : "Unexpected server error." });
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

function createRoom(hostName) {
  const roomId = id();
  const player = createPlayer(hostName, 0);
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
    players: [player]
  };
  rooms.set(roomId, room);
  return room;
}

function createPlayer(name, index) {
  return {
    id: index,
    name,
    hand: [],
    groups: [[], [], []],
    discarded: null,
    ready: false
  };
}

function dealRound(room) {
  const deck = buildDeck();
  room.players = room.players.map((player, index) => ({
    ...player,
    id: index,
    hand: deck.splice(0, 10),
    groups: [[], [], []],
    discarded: null,
    ready: false
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

  if (allSameRank) return { type: "toak", score: values[0], strength: 5 };
  if (isSequence && allSameSuit) return { type: "pure", score: sequenceHigh, strength: 4 };
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
      room.players[winner].groups[room.battleGroupIndex]
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
        : [[], [], []]
    }))
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

function fail(socket, message) {
  send(socket, "error", { message });
}

function roomFor(socket) {
  const meta = clientMeta.get(socket);
  return meta ? rooms.get(meta.roomId) : null;
}

function requireRoom(socket) {
  const room = roomFor(socket);
  if (!room) fail(socket, "Room not found.");
  return room;
}

function requirePlayer(socket, room) {
  const meta = clientMeta.get(socket);
  if (!meta) {
    fail(socket, "Player not registered.");
    return null;
  }
  return room.players[meta.playerIndex];
}

wss.on("connection", (socket) => {
  socket.on("message", (raw) => {
    let message;
    try {
      message = JSON.parse(String(raw));
    } catch {
      fail(socket, "Invalid JSON.");
      return;
    }

    const { type, payload = {} } = message;

    if (type === "create_room") {
      const room = createRoom(payload.name || "Host");
      clientMeta.set(socket, { roomId: room.id, playerIndex: 0 });
      send(socket, "room_created", { roomId: room.id, playerIndex: 0 });
      broadcastRoom(room);
      return;
    }

    if (type === "join_room") {
      const room = rooms.get(String(payload.roomId || "").toUpperCase());
      if (!room) return fail(socket, "Room not found.");
      if (room.players.length >= 4) return fail(socket, "Room is full.");
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
      if (meta.playerIndex !== 0) return fail(socket, "Only the host can start the game.");
      if (room.players.length < 2) return fail(socket, "Need at least 2 players.");
      room.scores = room.players.map(() => 0);
      room.groupsWon = room.players.map(() => 0);
      room.round = 1;
      dealRound(room);
      broadcastRoom(room);
      return;
    }

    if (type === "move_card_to_group") {
      if (room.phase !== "arrange") return fail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return fail(socket, "Not your turn.");
      const { cardIndex, groupIndex } = payload;
      if (!Number.isInteger(cardIndex) || !Number.isInteger(groupIndex)) return fail(socket, "Invalid move.");
      if (groupIndex < 0 || groupIndex > 2) return fail(socket, "Invalid group.");
      if (!player.hand[cardIndex]) return fail(socket, "Card not found.");
      if (player.groups[groupIndex].length >= 3) return fail(socket, "Group is full.");
      const [card] = player.hand.splice(cardIndex, 1);
      player.groups[groupIndex].push(card);
      broadcastRoom(room);
      return;
    }

    if (type === "discard_card") {
      if (room.phase !== "arrange") return fail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return fail(socket, "Not your turn.");
      const { cardIndex } = payload;
      if (player.discarded) return fail(socket, "Card already discarded.");
      if (player.groups.some((group) => group.length !== 3)) return fail(socket, "Finish all groups first.");
      const [card] = player.hand.splice(cardIndex, 1);
      if (!card) return fail(socket, "Card not found.");
      player.discarded = card;
      broadcastRoom(room);
      return;
    }

    if (type === "confirm_ready") {
      if (room.phase !== "arrange") return fail(socket, "Not in arrange phase.");
      if (meta.playerIndex !== room.activeArrangePlayerIndex) return fail(socket, "Not your turn.");
      if (player.groups.some((group) => group.length !== 3) || !player.discarded) {
        return fail(socket, "Groups or discard incomplete.");
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
      if (room.phase !== "battle") return fail(socket, "Not in battle phase.");
      room.battleRevealCount += 1;
      broadcastRoom(room);
      return;
    }

    if (type === "score_group") {
      if (room.phase !== "battle") return fail(socket, "Not in battle phase.");
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
  });

  socket.on("close", () => {
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
      console.log(`Trio Clash auth and multiplayer server running on :${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Failed to initialize database schema.", error);
    process.exit(1);
  });
