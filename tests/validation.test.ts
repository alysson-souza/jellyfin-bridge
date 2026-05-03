import test from "node:test";
import assert from "node:assert/strict";
import type { BridgeConfig } from "../src/config.js";
import { validateUpstreams } from "../src/validation.js";

test("validates configured upstream reachability and mapped library ids", async () => {
  const client = new FakeClient({
    "main:/System/Info/Public?": { Version: "10.11.8" },
    "main:/Users?": [{ Id: "user-a" }],
    "main:/UserViews?UserId=user-a": {
      Items: [{ Id: "library-a", Name: "Movies" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });

  await validateUpstreams(config, client);

  assert.deepEqual(client.requests, [
    "main:/System/Info/Public?",
    "main:/Users?",
    "main:/UserViews?UserId=user-a"
  ]);
});

test("fails startup validation when an upstream has no users for library discovery", async () => {
  const client = new FakeClient({
    "main:/System/Info/Public?": { Version: "10.11.8" },
    "main:/Users?": []
  });

  await assert.rejects(() => validateUpstreams(config, client), /Upstream main returned no users/);
});

test("fails startup validation when a mapped upstream library is missing", async () => {
  const client = new FakeClient({
    "main:/System/Info/Public?": { Version: "10.11.8" },
    "main:/Users?": [{ Id: "user-a" }],
    "main:/UserViews?UserId=user-a": {
      Items: [{ Id: "library-b", Name: "Other" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });

  await assert.rejects(() => validateUpstreams(config, client), /Mapped library main:library-a was not found/);
});

const config: BridgeConfig = {
  server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
  auth: { users: [{ name: "alice", passwordHash: "hash" }] },
  upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
  libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
};

class FakeClient {
  readonly requests: string[] = [];

  constructor(private readonly responses: Record<string, unknown>) {}

  async json<T>(serverId: string, path: string, init: { query?: Record<string, string | number | boolean | undefined> } = {}): Promise<T> {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(init.query ?? {})) {
      if (value !== undefined) query.set(key, String(value));
    }
    const request = `${serverId}:${path}?${query.toString()}`;
    this.requests.push(request);
    const response = this.responses[request];
    if (!response) throw new Error(`Unexpected request ${request}`);
    return response as T;
  }
}
