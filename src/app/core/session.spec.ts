import { readSessionId, rotateSessionId } from './session';

/** Minimal in-memory Storage, so the test never touches the real localStorage. */
const createStorage = (): Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> => {
  const values = new Map<string, string>();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => void values.set(key, value),
    removeItem: (key) => void values.delete(key),
  };
};

describe('session ids', () => {
  it('creates an id the first time and reuses it afterwards', () => {
    const storage = createStorage();

    const first = readSessionId(storage);
    const second = readSessionId(storage);

    expect(first).toBe(second);
    expect(first).toMatch(/^[a-zA-Z0-9_-]{4,48}$/);
  });

  it('replaces an id that the BFF would reject', () => {
    const storage = createStorage();
    storage.setItem('streamlens.session.id', 'not a valid id!');

    const sessionId = readSessionId(storage);

    expect(sessionId).not.toBe('not a valid id!');
    expect(sessionId).toMatch(/^[a-zA-Z0-9_-]{4,48}$/);
  });

  it('rotates the id, which is how "start over" creates a new consumer group', () => {
    const storage = createStorage();
    const first = readSessionId(storage);

    const second = rotateSessionId(storage);

    expect(second).not.toBe(first);
    expect(readSessionId(storage)).toBe(second);
  });
});
