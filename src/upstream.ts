import { request } from "undici";
import type { UpstreamConfig } from "./config.js";

export interface UpstreamResponse {
  statusCode: number;
  headers: Record<string, string | string[] | undefined>;
  body: {
    json(): Promise<unknown>;
    text?(): Promise<string>;
  } | unknown;
}

type RequestFunction = (url: URL, options: Parameters<typeof request>[1]) => Promise<UpstreamResponse>;

export interface UpstreamClientOptions {
  request?: RequestFunction;
  retries?: number;
  timeoutMs?: number;
}

export class UpstreamClient {
  private readonly upstreams: Map<string, UpstreamConfig>;
  private readonly request: RequestFunction;
  private readonly retries: number;
  private readonly timeoutMs: number;

  constructor(upstreams: UpstreamConfig[], options: UpstreamClientOptions = {}) {
    this.upstreams = new Map(upstreams.map((upstream) => [upstream.id, upstream]));
    this.request = options.request ?? ((url, options) => request(url, options));
    this.retries = options.retries ?? 2;
    this.timeoutMs = options.timeoutMs ?? 30_000;
  }

  async json<T>(serverId: string, path: string, init: { method?: string; body?: unknown; query?: Record<string, string | number | boolean | undefined> } = {}): Promise<T> {
    const response = await this.raw(serverId, path, init);
    if (!hasJsonBody(response.body)) {
      throw new Error(`Upstream ${serverId} returned a non-JSON body for ${path}`);
    }
    return response.body.json() as Promise<T>;
  }

  async raw(serverId: string, path: string, init: { method?: string; body?: unknown; query?: Record<string, string | number | boolean | undefined>; headers?: Record<string, string> } = {}) {
    const upstream = this.upstream(serverId);
    const url = buildUpstreamUrl(upstream.url, path, init.query);
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.retries; attempt += 1) {
      try {
        const response = await this.request(url, {
          method: init.method ?? "GET",
          body: init.body === undefined ? undefined : JSON.stringify(init.body),
          headers: {
            "X-Emby-Token": upstream.token,
            ...(init.body === undefined ? {} : { "Content-Type": "application/json" }),
            ...init.headers
          },
          bodyTimeout: this.timeoutMs,
          headersTimeout: this.timeoutMs
        });
        if (response.statusCode >= 200 && response.statusCode < 300) {
          return response;
        }
        if (!isRetryableStatus(response.statusCode) || attempt === this.retries) {
          throw new Error(`Upstream ${serverId} returned HTTP ${response.statusCode} for ${path}`);
        }
        lastError = new Error(`Upstream ${serverId} returned HTTP ${response.statusCode} for ${path}`);
      } catch (error) {
        lastError = error;
        if (isHttpError(error)) {
          throw error;
        }
        if (attempt === this.retries) {
          throw normalizeUpstreamError(serverId, path, error);
        }
      }
    }
    throw normalizeUpstreamError(serverId, path, lastError);
  }

  private upstream(serverId: string): UpstreamConfig {
    const upstream = this.upstreams.get(serverId);
    if (!upstream) {
      throw new Error(`Unknown upstream ${serverId}`);
    }
    return upstream;
  }
}

function isHttpError(error: unknown): boolean {
  return error instanceof Error && /^Upstream .+ returned HTTP \d+ /.test(error.message);
}

function isRetryableStatus(statusCode: number): boolean {
  return statusCode === 408 || statusCode === 429 || statusCode >= 500;
}

function normalizeUpstreamError(serverId: string, path: string, error: unknown): Error {
  if (error instanceof Error && error.message.startsWith(`Upstream ${serverId} returned HTTP`)) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  return new Error(`Upstream ${serverId} request failed for ${path}: ${message}`);
}

function hasJsonBody(body: unknown): body is { json(): Promise<unknown> } {
  return body !== null && typeof body === "object" && "json" in body && typeof body.json === "function";
}

export function buildUpstreamUrl(baseUrl: string, path: string, query: Record<string, string | number | boolean | undefined> = {}): URL {
  assertSafeUpstreamPath(path);
  const base = new URL(baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`);
  const basePath = base.pathname.replace(/\/+$/, "");
  const requestPath = path.replace(/^\/+/, "");
  base.pathname = `${basePath}/${requestPath}`.replace(/\/{2,}/g, "/");
  base.search = "";
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined) {
      base.searchParams.set(key, String(value));
    }
  }
  return base;
}

function assertSafeUpstreamPath(path: string): void {
  for (const segment of path.split("/")) {
    if (segment.length === 0) continue;
    if (segment.includes("\\")) {
      throw new Error(`Unsafe upstream path ${path}`);
    }

    let decoded: string;
    try {
      decoded = decodeURIComponent(segment);
    } catch {
      throw new Error(`Unsafe upstream path ${path}`);
    }

    if (decoded === "." || decoded === ".." || decoded.includes("/") || decoded.includes("\\")) {
      throw new Error(`Unsafe upstream path ${path}`);
    }
  }
}
