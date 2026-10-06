import {test,expect,vi} from 'vitest';
import {RelayServer} from '@cgp/relay/src/server';
import {MemoryStore} from '@cgp/relay/src/store';
test('a blocked authoritative plugin initializer is identified without bypassing its readiness gate',async()=>{
 let release!:()=>void;const blocked=new Promise<void>(resolve=>release=resolve),messages:string[]=[];
 const log=vi.spyOn(console,'info').mockImplementation((...args)=>{messages.push(args.join(' '));});
 const relay=new RelayServer(0,new MemoryStore(),[{name:'blocked-fixture',onInit:()=>blocked}],{enableDefaultPlugins:false});
 try{
  while(!relay.getPort())await new Promise(resolve=>setTimeout(resolve,10));
  let settled=false;const health=fetch(`http://127.0.0.1:${relay.getPort()}/healthz`).then(response=>{settled=true;return response;});
  await vi.waitFor(()=>expect(messages.some(message=>message.includes('[relay-plugin-init] plugin=blocked-fixture phase=onInit state=pending'))).toBe(true),{timeout:12000,interval:100});
  expect(settled).toBe(false);release();expect((await health).status).toBe(200);
  expect(messages.some(message=>message.includes('plugin=blocked-fixture phase=onInit state=complete'))).toBe(true);
 }finally{release();await relay.close();log.mockRestore();}
},20000);
