/** Community projection is repairable; signed library storage is authoritative. */
export class StaticShardCommunityQueue<T extends { id: string; version: string }> {
  private readonly pending = new Map<string,T>();
  private activeKey?: string;
  private scheduled = false;
  private closed = false;
  private readonly waiters: Array<()=>void> = [];
  constructor(private readonly register: (release:T)=>Promise<unknown>, private readonly failed:(release:T,error:unknown)=>void, private readonly capacity=64) {
    if(!Number.isSafeInteger(capacity)||capacity<1||capacity>256)throw new Error('Invalid community repair capacity');
  }
  enqueue(release:T): void {
    const key=release.id+'@'+release.version;
    if(this.closed||this.activeKey===key||this.pending.has(key))return;
    if(this.pending.size+(this.activeKey?1:0)>=this.capacity){this.failed(release,new Error('Community repair queue full; durable release will be repaired on restart'));return;}
    this.pending.set(key,release);
    if(!this.scheduled){this.scheduled=true;setImmediate(()=>{void this.drain();});}
  }
  private async drain():Promise<void>{
    try{
      while(!this.closed&&this.pending.size){
        const [key,release]=this.pending.entries().next().value!;this.pending.delete(key);this.activeKey=key;
        try{await this.register(release);}catch(error){this.failed(release,error);}
        this.activeKey=undefined;
        await new Promise<void>(resolve=>setImmediate(resolve));
      }
    }finally{this.scheduled=false;this.activeKey=undefined;for(const resolve of this.waiters.splice(0))resolve();}
  }
  idle():Promise<void>{return this.scheduled?new Promise(resolve=>this.waiters.push(resolve)):Promise.resolve();}
  close():void{this.closed=true;this.pending.clear();}
}

export async function publishDurableStaticLibrary<T extends {type:string}>(release:T,actions:{persist:(release:T)=>Promise<void>;remember:(release:T)=>void;enqueue:(release:T)=>void}):Promise<T>{
  if(release.type!=='library')throw new Error('Deferred community registration is library-only');
  await actions.persist(release);
  actions.remember(release);
  actions.enqueue(release);
  return release;
}
