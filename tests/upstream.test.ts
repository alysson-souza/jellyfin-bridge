import test from "node:test";
import assert from "node:assert/strict";
import { UpstreamClient, buildUpstreamUrl } from "../src/upstream.js";

test("builds upstream URLs without dropping a configured base path", () => {
  const url = buildUpstreamUrl("https://example.com/jellyfin/", "/UserViews", {
    UserId: "abc",
    IncludeExternalContent: false
  });

  assert.equal(url.toString(), "https://example.com/jellyfin/UserViews?UserId=abc&IncludeExternalContent=false");
});

test("builds upstream URLs for root-mounted servers", () => {
  const url = buildUpstreamUrl("https://example.com", "/System/Info/Public");

  assert.equal(url.toString(), "https://example.com/System/Info/Public");
});

test("rejects upstream paths that would normalize outside the intended endpoint", () => {
  assert.throws(
    () => buildUpstreamUrl("https://example.com/jellyfin", "/Items/item/Images/Primary/../../../../System/Info"),
    /Unsafe upstream path/
  );
  assert.throws(
    () => buildUpstreamUrl("https://example.com/jellyfin", "/Items/item/Images/%2e%2e/System/Info"),
    /Unsafe upstream path/
  );
});

test("retries transient upstream request failures", async () => {
  let attempts = 0;
  const client = new UpstreamClient(
    [{ id: "main", name: "Main", url: "https://example.com/jellyfin", token: "token" }],
    {
      request: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("socket closed");
        return response(200, { ok: true });
      },
      retries: 1
    }
  );

  const body = await client.json<{ ok: boolean }>("main", "/System/Info/Public");

  assert.equal(body.ok, true);
  assert.equal(attempts, 2);
});

test("does not retry non-transient upstream http failures", async () => {
  let attempts = 0;
  const client = new UpstreamClient(
    [{ id: "main", name: "Main", url: "https://example.com", token: "token" }],
    {
      request: async () => {
        attempts += 1;
        return response(401, { error: "bad token" });
      },
      retries: 2
    }
  );

  await assert.rejects(() => client.json("main", "/Users"), /Upstream main returned HTTP 401/);
  assert.equal(attempts, 1);
});

test("upstream clients do not expose mutable config replacement", () => {
  const client = new UpstreamClient([{ id: "main", name: "Main", url: "https://example.com", token: "token" }]);

  assert.equal("replaceUpstreams" in client, false);
});

test("new upstream client instances use updated configuration for future requests", async () => {
  const seen: Array<{ url: string; token: unknown }> = [];
  const options = {
    request: async (url: URL, options: any) => {
      const headers = options?.headers as Record<string, unknown> | undefined;
      seen.push({ url: url.toString(), token: headers?.["X-Emby-Token"] });
      return response(200, { ok: true });
    }
  };
  const oldClient = new UpstreamClient(
    [{ id: "main", name: "Main", url: "https://old.example.com", token: "old-token" }],
    options
  );
  const newClient = new UpstreamClient(
    [{ id: "main", name: "Main", url: "https://new.example.com/jellyfin", token: "new-token" }],
    options
  );

  await oldClient.json("main", "/System/Ping");
  await newClient.json("main", "/System/Ping");

  assert.deepEqual(seen, [
    { url: "https://old.example.com/System/Ping", token: "old-token" },
    { url: "https://new.example.com/jellyfin/System/Ping", token: "new-token" }
  ]);
});

function response(statusCode: number, body: unknown) {
  return {
    statusCode,
    headers: {},
    body: {
      json: async () => body,
      text: async () => JSON.stringify(body)
    }
  };
}
