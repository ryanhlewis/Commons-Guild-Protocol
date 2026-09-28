import dns from 'node:dns';
import {syncBuiltinESMExports} from 'node:module';
// Fixture process only: preserve requested hostname and TLS SNI/verification.
const original=dns.lookup.bind(dns),allowed=new Set<string>();
const resolver=new dns.Resolver();resolver.setServers(['1.1.1.1','8.8.8.8']);
const observations:any[]=[];
(dns as any).lookup=(hostname:string,options:any,callback:any)=>{
 if(typeof options==='function'){callback=options;options={};}
 return original(hostname,options,(error:any,address:any,family:any)=>{
  if(!error||!allowed.has(hostname))return callback(error,address,family);
  resolver.resolve4(hostname,(fallbackError,addresses)=>{
   if(fallbackError||!addresses?.length){observations.push({hostname,defaultError:error.code,fallbackError:fallbackError?.code,resolvers:["1.1.1.1","8.8.8.8"],at:new Date().toISOString()});return callback(error,address,family);}
   observations.push({hostname,defaultError:error.code,resolvers:['1.1.1.1','8.8.8.8'],addresses,at:new Date().toISOString()});
   return options?.all?callback(null,addresses.map(address=>({address,family:4}))):callback(null,addresses[0],4);
  });
 });
};
syncBuiltinESMExports();
export function allowFixtureHostname(url:string){const hostname=new URL(url).hostname;if(!hostname.endsWith('.trycloudflare.com'))throw Error('Only exact temporary fixture hostname allowed');allowed.add(hostname);}
export function fixtureDnsObservations(){return observations;}
