import {expect,test,vi} from 'vitest';
import {traceAsyncPhase} from '../../relay/src/async_phase_trace.js';
test('a blocked authoritative operation reports pending without releasing its gate',async()=>{
 vi.useFakeTimers();try{let resolve!:(value:number)=>void;const records:any[]=[];let finished=false;
 const result=traceAsyncPhase('getState',()=>new Promise<number>(r=>resolve=r),v=>records.push(v),1000).then(v=>{finished=true;return v;});
 await vi.advanceTimersByTimeAsync(1001);expect(finished).toBe(false);expect(records.map(v=>v.state)).toEqual(['pending']);
 resolve(7);expect(await result).toBe(7);expect(records.map(v=>v.state)).toEqual(['pending','complete']);await vi.advanceTimersByTimeAsync(10000);expect(records).toHaveLength(2);
 }finally{vi.useRealTimers();}
});
test('failure and diagnostic failure preserve the original operation result',async()=>{
 const error=new Error('original');const records:any[]=[];await expect(traceAsyncPhase('publish',async()=>{throw error},v=>records.push(v))).rejects.toBe(error);expect(records.map(v=>v.state)).toEqual(['failed']);
 expect(await traceAsyncPhase('publish',async()=>9,()=>{throw Error('logging')})).toBe(9);
});
