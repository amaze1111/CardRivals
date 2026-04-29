import { createMatchmakingManager } from "./backend_matchmaking.js";

export function createMatchmakingRoutes({
  matchmaking = createMatchmakingManager(),
  requireAuth,
  parseBody,
  fail,
  json,
  buildDisplayName,
  MATCH_ENTRY_FEES,
} = {}) {
  if (!requireAuth || !parseBody || !fail || !json) {
    throw new Error("Missing required server helpers (requireAuth/parseBody/fail/json).");
  }
  if (!buildDisplayName) {
    throw new Error("Missing buildDisplayName(user).");
  }
  if (!MATCH_ENTRY_FEES) {
    throw new Error("Missing MATCH_ENTRY_FEES mapping.");
  }

  async function handleMatchmakingStatus(req, res) {
    const session = await requireAuth(req, res);
    if (!session) return;

    const url = new URL(req.url, "http://localhost");
    const matchId = url.searchParams.get("matchId") || "";
    if (!matchId) return fail(res, 400, "matchId is required.");

    const status = matchmaking.getStatus({ matchId });
    if (!status) return fail(res, 404, "Match not found.");

    const you = status.players.find((p) => p.userId === session.userId);
    const opponentDisplayNames = status.players
      .filter((p) => p.userId !== session.userId)
      .map((p) => p.displayName);

    json(res, 200, {
      matchId: status.matchId,
      playerCount: status.playerCount,
      you: you ? { displayName: you.displayName } : null,
      opponentDisplayNames,
    });
  }

  async function handleMatchmakingJoin(req, res, joinArgs) {
    // NOTE: joinArgs is expected to be computed by your existing coin-deduct transaction:
    // { session, playerCount, entryFee, rewardPool, updatedCoins }
    const { session, playerCount, entryFee, rewardPool, updatedCoins } = joinArgs;
    const displayName = session?.displayName || "Player";
    matchmaking.join({
      req,
      res,
      userId: session.userId,
      displayName,
      playerCount,
      entryFee,
      rewardPool,
      coinBalance: updatedCoins,
      isRanked: true,
    });
  }

  return {
    matchmaking,
    handleMatchmakingStatus,
    handleMatchmakingJoin,
  };
}

