/**
 * A ~60 line router. The BFF exposes six endpoints — a framework would be dead
 * weight, and Node's http server already gives us streaming responses, which is
 * what the SSE endpoint actually needs.
 */
import { config } from './config.mjs';
import { log } from './log.mjs';

const CORS = {
  'Access-Control-Allow-Origin': config.corsOrigin,
  'Access-Control-Allow-Headers': 'content-type',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
};

export const json = (res, status, body) => {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    ...CORS,
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(payload),
    'Cache-Control': 'no-store',
  });
  res.end(payload);
};

/** Reads and parses a JSON body, with a hard size cap so a stray request cannot OOM us. */
export const readJsonBody = async (req, limitBytes = 64 * 1024) => {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;
    if (size > limitBytes) {
      throw Object.assign(new Error('payload too large'), { statusCode: 413 });
    }
    chunks.push(chunk);
  }

  if (size === 0) return {};

  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw Object.assign(new Error('invalid JSON body'), { statusCode: 400 });
  }
};

export const createRouter = () => {
  const routes = [];

  const register = (method, path, handler) => routes.push({ method, path, handler });

  const route = async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);

    if (req.method === 'OPTIONS') {
      res.writeHead(204, CORS);
      res.end();
      return;
    }

    const match = routes.find(
      (entry) => entry.method === req.method && entry.path === url.pathname,
    );
    if (!match) {
      json(res, 404, {
        error: 'not_found',
        path: url.pathname,
        routes: routes.map((entry) => `${entry.method} ${entry.path}`),
      });
      return;
    }

    const startedAt = Date.now();
    try {
      await match.handler(req, res, url);
      log.debug('http', { method: req.method, path: url.pathname, ms: Date.now() - startedAt });
    } catch (error) {
      log.error('route failed', { method: req.method, path: url.pathname, error: error.message });
      if (res.headersSent) {
        res.end();
        return;
      }
      json(res, error.statusCode ?? 500, { error: 'internal_error', message: error.message });
    }
  };

  return {
    get: (path, handler) => register('GET', path, handler),
    post: (path, handler) => register('POST', path, handler),
    route,
    list: () => routes.map((entry) => `${entry.method} ${entry.path}`),
  };
};
