import test from "node:test";
import assert from "node:assert/strict";
import { expandEnv, parseConfig } from "../src/config.js";

test("expands environment variables in config values", () => {
  const raw = "token: ${TOKEN}\nfallback: ${MISSING:-default}\n";

  assert.equal(expandEnv(raw, { TOKEN: "secret" }), "token: secret\nfallback: default\n");
});

test("rejects missing environment variables", () => {
  assert.throws(() => expandEnv("token: ${TOKEN}", {}), /Missing environment variable TOKEN/);
});

test("validates duplicate upstream library mappings", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
libraries:
  - id: movies
    name: Movies
    collectionType: movies
    sources:
      - server: main
        libraryId: abc
  - id: other
    name: Other
    collectionType: movies
    sources:
      - server: main
        libraryId: abc
`;

  assert.throws(() => parseConfig(raw, {}), /main:abc is mapped more than once/);
});

test("validates unknown upstream references", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
libraries:
  - id: movies
    name: Movies
    collectionType: movies
    sources:
      - server: remote
        libraryId: abc
`;

  assert.throws(() => parseConfig(raw, {}), /unknown upstream remote/);
});

test("parses optional scan tuning", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
scan:
  pageSize: 750
  concurrency: 3
  incrementalSafetySeconds: 30
  onStart: true
  intervalMinutes: 15
  fullScanIntervalMinutes: 1440
`;

  const config = parseConfig(raw, {});

  assert.deepEqual(config.scan, {
    pageSize: 750,
    concurrency: 3,
    incrementalSafetySeconds: 30,
    onStart: true,
    intervalMinutes: 15,
    fullScanIntervalMinutes: 1440
  });
});

test("parses optional startup validation tuning", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
startup:
  validateUpstreams: false
`;

  const config = parseConfig(raw, {});

  assert.deepEqual(config.startup, { validateUpstreams: false });
});
