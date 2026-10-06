import { expect, it, vi } from 'vitest';
import { createCatalogSearchPages } from './catalog_search_pages.js';
it('caches scoring and preserves the snapshot across publications and subsequent pages', () => {
  const rank = vi.fn(() => ['a', 'b', 'c']);
  const pages = createCatalogSearchPages(rank);
  const first = pages('query', 1, null, 1);
  expect(first.ok).toBe(true);
  if (!first.ok) return;
  expect(first.selected).toEqual(['a']);
  pages('query', 1, null, 1); expect(rank).toHaveBeenCalledTimes(1);
  rank.mockReturnValue(['new', 'a', 'b', 'c']);
  expect(pages('query', 1, first.page.nextCursor, 2)).toMatchObject({ selected: ['b'] });
  const fresh = pages('query', 1, null, 2);
  expect(fresh).toMatchObject({ selected: ['new'] });
  expect(pages('different query', 1, first.page.nextCursor, 2)).toMatchObject({ ok: false, status: 400 });
  expect(pages('query', 1, 'garbage', 2)).toMatchObject({ ok: false, status: 400 });
});
it('paginates every match past the former 64-result ceiling without duplicates', () => {
  const rows = Array.from({ length: 175 }, (_, i) => i);
  const pages = createCatalogSearchPages(() => rows);
  let cursor: string | null = null;
  const all: number[] = [];
  do {
    const result = pages('game', 48, cursor, 0);
    expect(result.ok).toBe(true);
    if (!result.ok) break;
    all.push(...result.selected); cursor = result.page.nextCursor;
  } while (cursor);
  expect(all).toEqual(rows);
});
