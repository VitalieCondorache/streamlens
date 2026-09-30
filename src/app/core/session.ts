const STORAGE_KEY = 'streamlens.session.id';

/**
 * The browser's session id doubles as the Kafka consumer group id
 * (`streamlens-ui-<sessionId>`), which is what makes a page refresh resume from
 * the committed offsets instead of replaying the topic. Keeping it in
 * localStorage is therefore a functional decision, not a caching one.
 */
export const readSessionId = (
  storage: Pick<Storage, 'getItem' | 'setItem'> = localStorage,
): string => {
  const existing = storage.getItem(STORAGE_KEY);
  if (existing !== null && /^[a-zA-Z0-9_-]{4,48}$/.test(existing)) return existing;

  const created = createId();
  storage.setItem(STORAGE_KEY, created);
  return created;
};

/** Starts a brand new consumer group (used by "start over"). */
export const rotateSessionId = (
  storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> = localStorage,
): string => {
  storage.removeItem(STORAGE_KEY);
  return readSessionId(storage);
};

const createId = (): string => {
  const random = globalThis.crypto?.randomUUID?.().replaceAll('-', '');
  if (random) return random.slice(0, 16);
  return `s${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`.slice(0, 16);
};
