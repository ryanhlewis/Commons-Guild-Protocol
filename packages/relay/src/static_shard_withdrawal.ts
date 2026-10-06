// Operator withdrawals override stored releases and future signed ingestion.
export function isStaticShardGameWithdrawn(id: string): boolean {
  if (process.env.CGP_STATIC_SHARD_WITHDRAW_AVERA === '1' && /(?:^avera(?:$|[-_])|[-_]avera(?:$|[-_]))/i.test(id)) return true;
  return (process.env.CGP_STATIC_SHARD_WITHDRAWN_GAME_IDS || '').split(',').includes(id);
}
