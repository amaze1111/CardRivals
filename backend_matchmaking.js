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
  const queues = new Map(); // playerCount -> Pending[]
  const matches = new Map(); // matchId -> { playerCount, players: [{ userId, displayName }], createdAtMs }

  function queueFor(playerCount) {
    const key = String(playerCount);
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

  function dropExistingForUser(queue, userId) {
    for (let i = queue.length - 1; i >= 0; i -= 1) {
      if (queue[i].userId !== userId) continue;
      const pending = queue[i];
      queue.splice(i, 1);
      clearTimeout(pending.timeoutId);
      pending.respond(409, { error: "Already queued for matchmaking." });
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

  function tryMatch(queue, playerCount) {
    if (queue.length < playerCount - 1) return null;
    return queue.splice(0, playerCount - 1);
  }

  function upsertMatch(matchId, playerCount, players) {
    matches.set(matchId, {
      matchId,
      playerCount,
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

      const queue = queueFor(playerCount);
      dropExistingForUser(queue, userId);

      const match = tryMatch(queue, playerCount);
      if (match) {
        const matchId = createMatchId();
        const matchPlayers = [
          { userId, displayName },
          ...match.map((pending) => ({ userId: pending.userId, displayName: pending.displayName })),
        ];
        upsertMatch(matchId, playerCount, matchPlayers);
        respondGroup(
          match,
          (pending) => ({
            matchId,
            playerCount,
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
        const payload = {
          matchId,
          playerCount,
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

      const respond = once((statusCode, payload) => {
        if (!res.writableEnded) {
          res.writeHead(statusCode, { "Content-Type": "application/json" });
          res.end(JSON.stringify(payload));
        }
      });

      const pending = {
        userId,
        displayName,
        coinBalance,
        respond,
        timeoutId: null,
      };

      pending.timeoutId = setTimeout(() => {
        remove(queue, pending);
        const matchId = createMatchId();
        upsertMatch(matchId, playerCount, [{ userId, displayName }]);
        const payload = {
          matchId,
          playerCount,
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
