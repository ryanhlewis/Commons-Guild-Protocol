/** Sanitized diagnostic: one pending marker and one completion marker per await. */
export async function traceAsyncPhase<T>(phase:string,work:()=>Promise<T>|T,report:(value:{phase:string;state:'pending'|'complete'|'failed';durationMs:number})=>void,pendingMs=10000):Promise<T>{
 const started=Date.now();const emit=(state:'pending'|'complete'|'failed')=>{try{report({phase,state,durationMs:Date.now()-started});}catch{/* diagnostics cannot change operation semantics */}};
 const timer=setTimeout(()=>emit('pending'),pendingMs);
 try{const value=await work();emit('complete');return value;}catch(error){emit('failed');throw error;}finally{clearTimeout(timer);}
}
