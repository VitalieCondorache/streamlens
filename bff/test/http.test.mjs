/**
 * The router, over real HTTP.
 *
 * The bridge has no web framework on purpose, which means the behaviour every
 * framework gives you for free (404s, body limits, error-to-status mapping) is
 * ours to get right — and therefore ours to test.
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import { after, before, describe, it } from 'node:test';

import { createRouter, json, readJsonBody } from '../src/http.mjs';

let router;
let server;
let origin;

before(async () => {
  router = createRouter();
  router.get('/api/ping', (req, res) => json(res, 200, { ok: true }));
  router.post('/api/echo', async (req, res) =>
    json(res, 200, { received: await readJsonBody(req) }),
  );
  router.post('/api/tiny', async (req, res) =>
    json(res, 200, { received: await readJsonBody(req, 32) }),
  );
  router.get('/api/boom', () => {
    throw new Error('kaboom');
  });
  router.get('/api/teapot', () => {
    throw Object.assign(new Error('nope'), { statusCode: 418 });
  });

  server = http.createServer((req, res) => void router.route(req, res));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
});

after(() => new Promise((resolve) => server.close(resolve)));

describe('router', () => {
  it('answers a registered route with JSON and cache headers', async () => {
    const response = await fetch(`${origin}/api/ping`);

    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type'), /application\/json/);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.equal(response.headers.get('access-control-allow-origin'), '*');
    assert.deepEqual(await response.json(), { ok: true });
  });

  it('lists the known routes when nothing matches', async () => {
    const response = await fetch(`${origin}/api/nope`);
    const body = await response.json();

    assert.equal(response.status, 404);
    assert.equal(body.error, 'not_found');
    assert.equal(body.path, '/api/nope');
    assert.ok(body.routes.includes('GET /api/ping'));
  });

  it('answers preflight without reaching a handler', async () => {
    const response = await fetch(`${origin}/api/ping`, { method: 'OPTIONS' });

    assert.equal(response.status, 204);
    assert.equal(response.headers.get('access-control-allow-methods'), 'GET,POST,OPTIONS');
  });

  it('reads a JSON body', async () => {
    const response = await fetch(`${origin}/api/echo`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ count: 500, topic: 'streamlens.orders' }),
    });

    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      received: { count: 500, topic: 'streamlens.orders' },
    });
  });

  it('treats an empty body as an empty object', async () => {
    const response = await fetch(`${origin}/api/echo`, { method: 'POST', body: '' });

    assert.deepEqual(await response.json(), { received: {} });
  });

  it('rejects malformed JSON with 400', async () => {
    const response = await fetch(`${origin}/api/echo`, { method: 'POST', body: '{ not json' });
    const body = await response.json();

    assert.equal(response.status, 400);
    assert.match(body.message, /invalid JSON body/);
  });

  it('refuses an oversized body with 413 instead of buffering it', async () => {
    const response = await fetch(`${origin}/api/tiny`, {
      method: 'POST',
      body: JSON.stringify({ padding: 'x'.repeat(4_096) }),
    });
    const body = await response.json();

    assert.equal(response.status, 413);
    assert.match(body.message, /payload too large/);
  });

  it('turns a thrown error into a 500 with a message', async () => {
    const response = await fetch(`${origin}/api/boom`);
    const body = await response.json();

    assert.equal(response.status, 500);
    assert.equal(body.error, 'internal_error');
    assert.equal(body.message, 'kaboom');
  });

  it('honours a statusCode an error carries', async () => {
    const response = await fetch(`${origin}/api/teapot`);

    assert.equal(response.status, 418);
    assert.equal((await response.json()).message, 'nope');
  });
});
