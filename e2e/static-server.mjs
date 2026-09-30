/**
 * Serves the production bundle as a plain static site.
 *
 * Two reasons this exists instead of reusing `ng serve`:
 *
 * 1. end-to-end tests then run against the artifact that actually gets deployed;
 * 2. with no `/api` backend on this origin the app takes its recorded-session path,
 *    which makes the suite deterministic — no Kafka, no Docker, no network.
 */
import { createServer } from 'node:http';
import { createReadStream, existsSync, statSync } from 'node:fs';
import { extname, join, normalize, resolve, sep } from 'node:path';

const root = resolve(import.meta.dirname, '..', 'dist', 'streamlens', 'browser');
const port = Number(process.env.E2E_PORT ?? 4173);

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.woff2': 'font/woff2',
  '.map': 'application/json; charset=utf-8',
};

if (!existsSync(join(root, 'index.html'))) {
  console.error(`Static bundle missing at ${root}. Run "npm run build" first.`);
  process.exit(1);
}

const server = createServer((request, response) => {
  const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);

  // No API on purpose: this is the deployment shape without a backend.
  if (url.pathname.startsWith('/api/')) {
    response.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
    response.end('{"error":"not_found","hint":"this static host has no BFF"}');
    return;
  }

  const requested = normalize(decodeURIComponent(url.pathname));
  let file = resolve(root, `.${requested}`);
  const insideRoot = file === root || file.startsWith(root + sep);

  if (!insideRoot || !existsSync(file) || statSync(file).isDirectory()) {
    // Single-page-app fallback, like the 404.html trick on GitHub Pages.
    file = join(root, 'index.html');
  }

  response.writeHead(200, {
    'content-type': TYPES[extname(file)] ?? 'application/octet-stream',
    'cache-control': 'no-store',
  });
  createReadStream(file).pipe(response);
});

server.listen(port, '127.0.0.1', () => {
  console.log(`static bundle served on http://127.0.0.1:${port} (root: ${root})`);
});
