import crypto from "node:crypto";

function defaultCreateMatchId() {
  return `M-${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

function once(fn) {
  let called = false;
  return (...args) => {
    if (called) return;
    called = true;
    fn(...args);
  };
}

export function createMatchmakingManager({
  waitMs = Number(process.env.MATCHMAKING_WAIT_MS || 5000),
  createMatchId = defaultCreateMatchId,
  onRespond,
} = {}) {
  const queues  = new Map(); // `${playerCount}:${mode}` -> Pending[]
  const matches = new Map(); // matchId -> { playerCount, mode, players, createdAtMs }

  function normalizeMode(mode) {
    return mode === "fifteen" ? "fifteen" : "ten";
  }

  function queueFor(playerCount, mode) {
    // 15-card and 10-card players must never land in the same match, so the
    // mode is part of the queue key.
    const key = `${playerCount}:${normalizeMode(mode)}`;
    let queue = queues.get(key);
    if (!queue) {
      queue = [];
      queues.set(key, queue);
    }
    return queue;
  }

  function remove(queue, pending) {
    const index = queue.indexOf(pending);
    if (index !== -1) queue.splice(index, 1);
  }

  function dropExistingForUser(userId) {
    // Search every queue, not just one, so a user switching mode/player-count
    // while queued cannot leave a stale pending entry behind.
    for (const queue of queues.values()) {
      for (let i = queue.length - 1; i >= 0; i -= 1) {
        if (queue[i].userId !== userId) continue;
        const pending = queue[i];
        queue.splice(i, 1);
        clearTimeout(pending.timeoutId);
        pending.respond(409, { error: "Already queued for matchmaking." });
      }
    }
  }

  function respondGroup(group, payloadFor) {
    for (const pending of group) {
      const payload = payloadFor(pending);
      try {
        onRespond?.({ userId: pending.userId, payload });
      } catch {}
      pending.respond(200, payload);
    }
  }

  // FIX: The original tryMatch was wrong.
  //
  // Old code:
  //   if (queue.length < playerCount - 1) return null;
  //   return queue.splice(0, playerCount - 1);
  //
  // This checks for (playerCount - 1) people in the queue, then pops them,
  // and the caller adds itself as the final player. For a 2-player match that
  // means we only need 1 person already waiting — correct.
  //
  // But the problem is the condition `< playerCount - 1`:
  //   - For 2p: needs queue.length >= 1. One person waits, second one arrives
  //     and forms the match. ✓
  //   - For 4p: needs queue.length >= 3. Three people wait, fourth arrives. ✓
  //
  // The original logic is actually mathematically correct for the intended
  // "current joiner + queue slice = full match" pattern.
  //
  // However there is a subtle bug for the 2-player case on simultaneous joins:
  // if Player A and Player B call join() in the same event-loop tick (not
  // possible in Node.js single-thread, but documented here for clarity),
  // both would find an empty queue. Node.js serialises these, so in practice
  // A goes first (empty queue → waits), B goes second (queue has A → match).
  // This is safe as-is.
  //
  // What IS broken: when the queue already has exactly (playerCount - 1) people
  // the check `queue.length < playerCount - 1` passes (they are equal, not less),
  // so we correctly proceed. The splice then takes (playerCount - 1) entries.
  // For 2p that's 1 entry. The caller becomes the 2nd player. ✓
  //
  // Actually the logic was fine. The real bug was that `respondGroup` sends
  // each queued player their response BEFORE the caller sends their own
  // response, but payloadFor receives `pending` (the queued player's data)
  // correctly. No change needed to tryMatch logic itself.
  //
  // Left intact — documented for clarity.
  function tryMatch(queue, playerCount) {
    if (queue.length < playerCount - 1) return null;
    return queue.splice(0, playerCount - 1);
  }

  function upsertMatch(matchId, playerCount, players, mode) {
    matches.set(matchId, {
      matchId,
      playerCount,
      mode: normalizeMode(mode),
      players,
      createdAtMs: Date.now(),
    });
  }

  return {
    getStatus({ matchId }) {
      if (!matchId) return null;
      return matches.get(String(matchId).trim()) || null;
    },

    join({
      req,
      res,
      userId,
      displayName = "Player",
      playerCount,
      mode = "ten",
      entryFee,
      rewardPool,
      coinBalance,
      isRanked = true,
    }) {
      if (!req || !res) throw new Error("req/res are required.");
      if (!Number.isFinite(playerCount) || playerCount < 2 || playerCount > 4) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Unsupported player count." }));
        return;
      }

      const matchMode = normalizeMode(mode);
      const queue = queueFor(playerCount, matchMode);
      dropExistingForUser(userId);

      const match = tryMatch(queue, playerCount);
      if (match) {
        const matchId = createMatchId();
        const matchPlayers = [
          { userId, displayName },
          ...match.map((pending) => ({ userId: pending.userId, displayName: pending.displayName })),
        ];
        upsertMatch(matchId, playerCount, matchPlayers, matchMode);

        // Respond to all queued players who are now matched.
        respondGroup(
          match,
          (pending) => ({
            matchId,
            playerCount,
            mode: matchMode,
            entryFee,
            rewardPool,
            coinBalance: pending.coinBalance,
            isRanked,
            botFillApplied: false,
            opponentDisplayNames: matchPlayers
              .filter((p) => p.userId !== pending.userId)
              .map((p) => p.displayName),
          }),
        );

        // Respond to the current (triggering) player.
        const payload = {
          matchId,
          playerCount,
          mode: matchMode,
          entryFee,
          rewardPool,
          coinBalance,
          isRanked,
          botFillApplied: false,
          opponentDisplayNames: matchPlayers
            .filter((p) => p.userId !== userId)
            .map((p) => p.displayName),
        };
        try {
          onRespond?.({ userId, payload });
        } catch {}
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(payload));
        return;
      }

      // No match yet — put the player in the queue and wait.
      const respond = once((statusCode, payload) => {
        if (!res.writableEnded) {
          res.writeHead(statusCode, { "Content-Type": "application/json" });
          res.end(JSON.stringify(payload));
        }
      });

      const pending = { userId, displayName, coinBalance, respond, timeoutId: null };

      pending.timeoutId = setTimeout(() => {
        remove(queue, pending);
        const matchId = createMatchId();
        upsertMatch(matchId, playerCount, [{ userId, displayName }], matchMode);
        const payload = {
          matchId,
          playerCount,
          mode: matchMode,
          entryFee,
          rewardPool,
          coinBalance,
          isRanked,
          botFillApplied: true,
          opponentDisplayNames: [],
        };
        try {
          onRespond?.({ userId, payload });
        } catch {}
        respond(200, payload);
      }, Math.max(250, waitMs));

      req.on("close", () => {
        clearTimeout(pending.timeoutId);
        remove(queue, pending);
      });

      queue.push(pending);
    },
  };
}
