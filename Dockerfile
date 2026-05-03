ARG NODE_VERSION=22

FROM node:${NODE_VERSION}-trixie-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM deps AS build
COPY tsconfig.json ./
COPY src ./src
RUN npm run build
RUN npm prune --omit=dev

FROM gcr.io/distroless/nodejs${NODE_VERSION}-debian13:nonroot AS runtime-base

FROM node:${NODE_VERSION}-trixie-slim AS runtime-rootfs
COPY --from=runtime-base / /runtime-rootfs/
RUN find /runtime-rootfs -xdev -perm /6000 -exec chmod a-s {} + \
  && for path in /runtime-rootfs/tmp /runtime-rootfs/var/tmp /runtime-rootfs/var/lock /runtime-rootfs/home/nonroot; do \
    if [ -e "$path" ]; then chmod -R a-w "$path"; fi; \
  done

FROM node:${NODE_VERSION}-trixie-slim AS runtime-dirs
RUN mkdir -p /runtime/config /runtime/data \
  && chown -R 65532:65532 /runtime/data

FROM scratch AS runtime
ENV NODE_ENV=production
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
COPY --from=runtime-rootfs /runtime-rootfs/ /
WORKDIR /app
COPY --from=build --chown=0:0 /app/package.json /app/package-lock.json ./
COPY --from=build --chown=0:0 /app/node_modules ./node_modules
COPY --from=build --chown=0:0 /app/dist ./dist
COPY --chown=0:0 config.example.yaml ./config.example.yaml
COPY --from=runtime-dirs --chown=0:0 /runtime/config /config
COPY --from=runtime-dirs --chown=65532:65532 /runtime/data /data
USER nonroot
EXPOSE 8096
VOLUME ["/data"]
ENTRYPOINT ["/nodejs/bin/node"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["/nodejs/bin/node", "dist/src/healthcheck.js"]
CMD ["dist/src/server.js", "--config", "/config/config.yaml", "--database", "/data/jellyfin-bridge.db"]
