import http from "node:http";

const url = process.env.JELLYFIN_BRIDGE_HEALTHCHECK_URL ?? "http://127.0.0.1:8096/System/Ping";

const request = http.get(url, (response) => {
  response.resume();
  process.exit(response.statusCode === 200 ? 0 : 1);
});

request.on("error", () => {
  process.exit(1);
});
