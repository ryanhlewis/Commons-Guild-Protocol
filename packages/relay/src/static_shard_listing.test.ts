import {describe, expect, it} from 'vitest';
import {staticShardListingAttribution} from './static_shard_listing.js';
describe('signed listing attribution', () => {
  it('preserves old attribution when optional fields are omitted', () => {
    expect(staticShardListingAttribution({title:'Legacy edit'})).toEqual({});
  });
  it('accepts artwork and author credit without transferring authority', () => {
    expect(staticShardListingAttribution({icon:'https://cdn.example/game.webp',creatorAvatar:'https://cdn.example/author.webp',creatorName:' Wuzzy ',creatorUsername:'Wuzzy',creatorId:'victim',publisher:'victim'})).toEqual({icon:'https://cdn.example/game.webp',creatorAvatar:'https://cdn.example/author.webp',creatorName:'Wuzzy',creatorUsername:'Wuzzy'});
  });
  it('rejects executable URLs, traversal and oversized credits', () => {
    for (const icon of ['javascript:alert(1)','../icon.png','//host/icon.png','https://user:secret@example.com/icon.png']) {expect(()=>staticShardListingAttribution({icon})).toThrow();expect(()=>staticShardListingAttribution({creatorAvatar:icon})).toThrow();}
    expect(()=>staticShardListingAttribution({creatorName:'x'.repeat(201)})).toThrow();
  });
  it('accepts signed preview metadata and rejects unsafe preview sources', () => {
    expect(staticShardListingAttribution({previewUrl:'https://www.youtube.com/watch?v=Hf6wewgVrFQ'})).toEqual({previewUrl:'https://www.youtube.com/watch?v=Hf6wewgVrFQ'});
    for (const previewUrl of ['javascript:alert(1)', '//host/video.mp4', '../video.mp4', 'https://user:pass@host/video.mp4']) expect(() => staticShardListingAttribution({previewUrl})).toThrow();
  });
});
