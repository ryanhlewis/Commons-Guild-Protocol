import { expect, test } from 'vitest';
import WebSocket from 'ws';
import { RelayServer } from '@cgp/relay/src/server';
import { MemoryStore } from '@cgp/relay/src/store';
import { generatePrivateKey, getPublicKey, hashObject, sign } from '@cgp/core';

test('accepts importer-signed private history and preserves owner and source attribution', async () => {
 const store = new MemoryStore(); const relay = new RelayServer(17893,store,[],{enableDefaultPlugins:false});
 const key = generatePrivateKey(); const author = getPublicKey(key);
 const guildId = hashObject({protocol:'hollow-import-guild/1',publisher:author,platform:'discord',scope:'fixture'});
 const channelId = hashObject({protocol:'hollow-import-channel/1',guildId,source:'general'});
 const socket = new WebSocket('ws://127.0.0.1:17893');
 await new Promise<void>((resolve,reject)=>{socket.once('open',resolve);socket.once('error',reject);});
 const publish = async (body:Record<string,unknown>) => {
  const createdAt = Date.now(); const clientEventId = crypto.randomUUID();
  const signature = await sign(key,hashObject({body,author,createdAt}));
  await new Promise<void>((resolve,reject)=>{
   const timeout = setTimeout(()=>{socket.off('message',receive);reject(Error('Relay acknowledgement timeout'));},5000);
   const receive = (raw:WebSocket.RawData) => {const [kind,result]=JSON.parse(raw.toString());if(result?.clientEventId!==clientEventId)return;clearTimeout(timeout);socket.off('message',receive);kind==='PUB_ACK'?resolve():reject(Error(result?.message||kind));};
   socket.on('message',receive);socket.send(JSON.stringify(['PUBLISH',{body,author,createdAt,signature,clientEventId}]));
  });
 };
 try {
  await publish({type:'GUILD_CREATE',guildId,name:'Imported community',access:'private',external:{importPlatform:'discord',sourceScope:'fixture',importedBy:author}});
  await publish({type:'CHANNEL_UPSERT',guildId,channelId,name:'general',kind:'text'});
  const messageId = hashObject({kind:'hollow-message',guildId,channelId,nonce:'import:source-message'});
  await publish({type:'MESSAGE',guildId,channelId,messageId,content:'Imported from discord · Source author · 2026-10-01\nHistorical content',external:{importedHistory:{protocol:'hollow-import-history/1',sourceHash:hashObject('source-message')}}});
  await publish({type:'INVITE_CREATE',guildId,inviteId:'fixture-invite',code:'fixture-invite',channelId,creatorId:author,createdAt:new Date().toISOString(),uses:0});
  const log = store.getLog(guildId);
  expect(log).toHaveLength(4); expect(log.every(event=>event.author===author)).toBe(true);
  expect(log[0].body.access).toBe('private'); expect(log[2].body.messageId).toBe(messageId);
  expect(log[2].body.content).toContain('Source author'); expect(log[2].body.external).toMatchObject({importedHistory:{protocol:'hollow-import-history/1'}});
 } finally {socket.close();await relay.close();}
},15000);
