export interface HlsRewriteOptions {
  bridgeBasePath: string;
  upstreamBasePath: string;
  authToken?: string;
  pathSegmentRewrites?: Map<string, string>;
  queryRewrites?: Array<{ names: string[]; ids: Map<string, string> }>;
}

export function rewriteHlsPlaylist(playlist: string, options: HlsRewriteOptions): string {
  return playlist
    .split(/(\r?\n)/)
    .map((part) => {
      if (part === "\n" || part === "\r\n" || part.length === 0) return part;
      if (part.startsWith("#")) return rewriteTagUris(part, options);
      return rewriteUri(part, options);
    })
    .join("");
}

function rewriteTagUris(line: string, options: HlsRewriteOptions): string {
  return line.replace(/URI="([^"]+)"/g, (_match, uri: string) => `URI="${rewriteUri(uri, options)}"`);
}

function rewriteUri(uri: string, options: HlsRewriteOptions): string {
  if (/^[a-z][a-z0-9+.-]*:/i.test(uri)) {
    try {
      const parsed = new URL(uri);
      if (parsed.pathname.startsWith(options.upstreamBasePath)) {
        const suffix = rewritePathSegment(parsed.pathname.slice(options.upstreamBasePath.length).replace(/^\/+/, ""), options);
        return appendPath(options.bridgeBasePath, suffix) + rewriteUriSuffix(`${parsed.search}${parsed.hash}`, options);
      }
    } catch {
      return uri;
    }
    return uri;
  }
  if (uri.startsWith("/")) {
    const [path, suffix] = splitUri(uri);
    if (path.startsWith(options.upstreamBasePath)) {
      const pathSuffix = rewritePathSegment(path.slice(options.upstreamBasePath.length).replace(/^\/+/, ""), options);
      return appendPath(options.bridgeBasePath, pathSuffix) + rewriteUriSuffix(suffix, options);
    }
    return uri;
  }
  const [path, suffix] = splitUri(uri);
  return appendPath(options.bridgeBasePath, rewritePathSegment(path, options)) + rewriteUriSuffix(suffix, options);
}

function appendPath(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}

function rewritePathSegment(path: string, options: HlsRewriteOptions): string {
  if (!options.pathSegmentRewrites?.size) return path;
  const trimmed = path.replace(/^\/+/, "");
  if (!trimmed) return path;
  const [segment, ...rest] = trimmed.split("/");
  const rewritten = options.pathSegmentRewrites.get(segment);
  return rewritten ? [rewritten, ...rest].join("/") : path;
}

function splitUri(uri: string): [string, string] {
  const index = uri.search(/[?#]/);
  return index === -1 ? [uri, ""] : [uri.slice(0, index), uri.slice(index)];
}

function rewriteUriSuffix(suffix: string, options: HlsRewriteOptions): string {
  const parsed = new URL(`http://bridge.local/${suffix}`);
  if (options.queryRewrites?.length) {
    for (const rewrite of options.queryRewrites) {
      for (const name of rewrite.names) {
        const value = parsed.searchParams.get(name);
        const rewritten = value ? rewrite.ids.get(value) : undefined;
        if (rewritten) parsed.searchParams.set(name, rewritten);
      }
    }
  }
  if (options.authToken) {
    parsed.searchParams.delete("api_key");
    parsed.searchParams.set("ApiKey", options.authToken);
  }
  return `${parsed.search}${parsed.hash}`;
}
