ARG NODE_VERSION=22

FROM node:${NODE_VERSION}-bookworm-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM deps AS build
COPY tsconfig.json ./
COPY src ./src
RUN npm run build
RUN npm prune --omit=dev

FROM node:${NODE_VERSION}-bookworm-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app/package.json /app/package-lock.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY config.example.yaml ./config.example.yaml
RUN useradd --system --create-home --home-dir /var/lib/jellyfin-bridge jellyfin-bridge \
  && mkdir -p /config /data \
  && chown -R jellyfin-bridge:jellyfin-bridge /config /data /app
USER jellyfin-bridge
EXPOSE 8096
VOLUME ["/config", "/data"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD node -e "fetch('http://127.0.0.1:8096/System/Ping').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "dist/src/server.js", "--config", "/config/config.yaml", "--database", "/data/jellyfin-bridge.db"]
