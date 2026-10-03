import { test } from 'node:test';
import assert from 'node:assert/strict';
import { zipSync } from 'fflate';
import { staticShardChunkReservation, staticShardZipReservation } from './static_shard_reservation.js';
const accepts = (name: string) => !name.startsWith('../');

test('small releases reserve their actual extraction sizes, including empty files', () => {
  const zip = zipSync({ 'index.html': new Uint8Array(200), 'empty': new Uint8Array() });
  assert.deepEqual(staticShardZipReservation(zip, {bytes:1000,files:10}, accepts), {bytes:200,files:2});
});
test('highly compressed payloads cannot bypass the extraction reservation', () => {
  const zip = zipSync({ 'large': new Uint8Array(10000) });
  assert.ok(zip.length < 1000);
  assert.throws(() => staticShardZipReservation(zip, {bytes:1000,files:10}, accepts), /extraction limits/);
});
test('multiple shards must consume one cumulative size and file budget', () => {
  const zip = zipSync({ 'file':new Uint8Array(200) });
  const first=staticShardZipReservation(zip,{bytes:300,files:2},accepts);
  assert.throws(()=>staticShardZipReservation(zip,{bytes:300-first.bytes,files:2-first.files},accepts),/extraction limits/);
  assert.throws(()=>staticShardZipReservation(zip,{bytes:1000,files:0},accepts),/extraction limits/);
});
test('ignored paths and directory entries match the extraction filter', () => {
  const zip=zipSync({'../ignored':new Uint8Array(1000),'directory/':new Uint8Array(),'directory/ok':new Uint8Array(40)});
  assert.deepEqual(staticShardZipReservation(zip,{bytes:40,files:1},accepts),{bytes:40,files:1});
});
test('large-file reconstruction reserves additional peak space and enforces its quota', () => {
  assert.equal(staticShardChunkReservation(undefined,1000,10),0);
  assert.equal(staticShardChunkReservation([{bytes:300,parts:['a','b']}],1000,10),300);
  assert.throws(()=>staticShardChunkReservation([{bytes:1001,parts:['a']}],1000,10),/quota/);
  assert.throws(()=>staticShardChunkReservation([{bytes:NaN,parts:['a']}],1000,10),/entry/);
  assert.throws(()=>staticShardChunkReservation([{bytes:10,parts:[]}],1000,10),/entry/);
});
