export interface HlsRewriteOptions {
  bridgeBasePath: string;
  upstreamBasePath: string;
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
        const suffix = parsed.pathname.slice(options.upstreamBasePath.length).replace(/^\/+/, "");
        return appendPath(options.bridgeBasePath, suffix) + parsed.search + parsed.hash;
      }
    } catch {
      return uri;
    }
    return uri;
  }
  if (uri.startsWith("/")) {
    if (uri.startsWith(options.upstreamBasePath)) {
      const suffix = uri.slice(options.upstreamBasePath.length).replace(/^\/+/, "");
      return appendPath(options.bridgeBasePath, suffix);
    }
    return uri;
  }
  return appendPath(options.bridgeBasePath, uri);
}

function appendPath(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}
