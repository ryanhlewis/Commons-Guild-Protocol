/** Optional signed listing fields. Attribution does not change publisher or creator authority. */
export function staticShardListingAttribution(claim: Record<string, unknown>) {
  const fields: Record<string, string | undefined> = {};
  for (const [key, limit] of [['icon', 2048], ['previewUrl', 2048], ['creatorAvatar', 2048], ['creatorName', 200], ['creatorUsername', 200], ['creatorBio', 8000]] as const) {
    if (!Object.prototype.hasOwnProperty.call(claim, key)) continue;
    if (typeof claim[key] !== 'string' || claim[key].length > limit) throw new Error(`Invalid listing ${key}.`);
    const value = claim[key].trim();
    if ((key === 'icon' || key === 'creatorAvatar' || key === 'previewUrl') && value) {
      if (/^https:\/\//i.test(value)) {
        const url = new URL(value);
        if (url.username || url.password) throw new Error('Invalid listing icon URL.');
      } else if (/[:\\?#\x00-\x1f]/.test(value) || value.startsWith('/') || value.split('/').some(part => !part || part === '.' || part === '..')) {
        throw new Error('Invalid listing icon path.');
      }
    }
    fields[key] = value || undefined;
  }
  return fields;
}
