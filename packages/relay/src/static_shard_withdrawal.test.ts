import test from 'node:test';
import assert from 'node:assert/strict';
import { isStaticShardGameWithdrawn } from './static_shard_withdrawal.js';

test('operator withdrawal covers Avera family IDs and explicit IDs while retaining other games', () => {
  const previousFamily = process.env.CGP_STATIC_SHARD_WITHDRAW_AVERA;
  const previousIds = process.env.CGP_STATIC_SHARD_WITHDRAWN_GAME_IDS;
  try {
    process.env.CGP_STATIC_SHARD_WITHDRAW_AVERA = '1';
    process.env.CGP_STATIC_SHARD_WITHDRAWN_GAME_IDS = 'custom-voxel-game';
    for (const id of ['avera','avera-test','eldaria-islands-v3-1-avera','custom-voxel-game']) assert.equal(isStaticShardGameWithdrawn(id), true);
    for (const id of ['eldaria-islands-v3-1-blockyard','nodeverse','luanti','primavera']) assert.equal(isStaticShardGameWithdrawn(id), false);
  } finally {
    if (previousFamily === undefined) delete process.env.CGP_STATIC_SHARD_WITHDRAW_AVERA;
    else process.env.CGP_STATIC_SHARD_WITHDRAW_AVERA = previousFamily;
    if (previousIds === undefined) delete process.env.CGP_STATIC_SHARD_WITHDRAWN_GAME_IDS;
    else process.env.CGP_STATIC_SHARD_WITHDRAWN_GAME_IDS = previousIds;
  }
});
