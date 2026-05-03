import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import YAML from "yaml";

test("local compose runs jellyfin-bridge with least-privilege container defaults", async () => {
  const compose = YAML.parse(await readFile("docker-compose.yml", "utf8"));
  const service = compose.services["jellyfin-bridge"];

  assert.equal(service.read_only, true);
  assert.ok(service.security_opt.includes("no-new-privileges:true"));
  assert.deepEqual(service.cap_drop, ["ALL"]);
  assert.equal(service.restart, "unless-stopped");
  assert.ok(service.volumes.includes("./config.yaml:/config/config.yaml:ro"));
  assert.ok(service.volumes.includes("jellyfin-bridge-data:/data:rw"));
});

test("runtime image does not make application files writable by the app user", async () => {
  const dockerfile = await readFile("Dockerfile", "utf8");

  assert.match(dockerfile, /FROM node:\$\{NODE_VERSION\}-trixie-slim AS deps/);
  assert.match(dockerfile, /FROM gcr\.io\/distroless\/nodejs\$\{NODE_VERSION\}-debian13:nonroot AS runtime-base/);
  assert.match(dockerfile, /FROM node:\$\{NODE_VERSION\}-trixie-slim AS runtime-rootfs/);
  assert.match(dockerfile, /FROM node:\$\{NODE_VERSION\}-trixie-slim AS runtime-dirs/);
  assert.match(dockerfile, /FROM scratch AS runtime/);
  assert.match(dockerfile, /USER nonroot/);
  assert.doesNotMatch(dockerfile, /chown\s+-R[^\n]*\/app/);
});

test("runtime image rootfs is cleaned before the final scratch stage", async () => {
  const dockerfile = await readFile("Dockerfile", "utf8");

  assert.match(dockerfile, /COPY --from=runtime-base \/ \/runtime-rootfs\//);
  assert.match(dockerfile, /find\s+\/runtime-rootfs\s+-xdev\s+-perm\s+\/6000\s+-exec\s+chmod\s+a-s/);
  assert.match(dockerfile, /COPY --from=runtime-rootfs \/runtime-rootfs\/ \//);
});

test("runtime image avoids shell-form startup hooks", async () => {
  const dockerfile = await readFile("Dockerfile", "utf8");

  assert.match(dockerfile, /ENTRYPOINT \["\/nodejs\/bin\/node"\]/);
  assert.match(dockerfile, /HEALTHCHECK[^\n]+CMD \["\/nodejs\/bin\/node", "-e"/);
  assert.doesNotMatch(dockerfile, /HEALTHCHECK[^\n]+CMD node -e/);
  assert.match(dockerfile, /CMD \["dist\/src\/server\.js", "--config", "\/config\/config\.yaml", "--database", "\/data\/jellyfin-bridge\.db"\]/);
});

test("docker build context excludes local secrets and generated state", async () => {
  const ignored = new Set((await readFile(".dockerignore", "utf8")).split(/\r?\n/).filter(Boolean));

  for (const pattern of [
    "node_modules",
    "dist",
    ".git",
    "*.db",
    "*.db-shm",
    "*.db-wal",
    "*.sqlite",
    "*.sqlite-shm",
    "*.sqlite-wal",
    ".env",
    ".env.*",
    ".npmrc",
    ".npmrc.*",
    "config.yaml",
    "config.docker.yaml",
    "*.local.yaml",
    "data/"
  ]) {
    assert.ok(ignored.has(pattern), `${pattern} should be excluded from Docker build context`);
  }
});

test("image audit rejects root users by declaration and runtime uid", async () => {
  const script = await readFile("scripts/audit-docker-image.sh", "utf8");

  assert.match(script, /image_user_name="\$\{image_user%%:\*\}"/);
  assert.match(script, /case "\$image_user_name" in[\s\S]*"" \| "0" \| "root"/);
  assert.match(script, /process\.getuid\(\) === 0/);
});

test("image audit rejects forbidden repo state outside /app too", async () => {
  const script = await readFile("scripts/audit-docker-image.sh", "utf8");

  assert.match(script, /path_basename="\$\{path##\*\/\}"/);
  assert.match(script, /case "\$path_basename" in[\s\S]*\.env[\s\S]*\.npmrc[\s\S]*config\.yaml/);
  assert.match(script, /case "\$path" in[\s\S]*\*\/\.git\/\*/);
});

test("image audit checks layer metadata for file capabilities", async () => {
  const script = await readFile("scripts/audit-docker-image.sh", "utf8");

  assert.match(script, /docker save "\$image" -o "\$image_archive"/);
  assert.match(script, /security\\.capability/);
});

test("image audit requires /data to be the only writable default runtime path", async () => {
  const script = await readFile("scripts/audit-docker-image.sh", "utf8");

  assert.match(script, /const allowedWritablePrefixes = \[['"]\/data['"]\]/);
  assert.match(script, /walk\(['"]\/['"]\)/);
  assert.match(script, /runtime user can write only \/data by default/);
});

test("container workflow publishes the same platform images that were audited", async () => {
  const workflow = YAML.parse(await readFile(".github/workflows/container.yml", "utf8"));
  const steps = workflow.jobs.build.steps;
  const buildPushSteps = steps.filter((step: { uses?: string }) => step.uses === "docker/build-push-action@v6");

  assert.equal(buildPushSteps.length, 2);
  assert.deepEqual(
    buildPushSteps.map((step: { name: string }) => step.name),
    ["Build linux/amd64 image for audit", "Build linux/arm64 image for audit"]
  );
  assert.ok(buildPushSteps.every((step: { with: { load?: boolean; push?: unknown } }) => step.with.load === true));
  assert.ok(buildPushSteps.every((step: { with: { push?: unknown } }) => step.with.push === undefined));

  const publishStep = steps.find((step: { name?: string }) => step.name === "Publish audited platform images");
  assert.ok(publishStep);
  assert.match(publishStep.run, /docker tag jellyfin-bridge:audit-amd64 "\$amd64_ref"/);
  assert.match(publishStep.run, /docker tag jellyfin-bridge:audit-arm64 "\$arm64_ref"/);
  assert.match(publishStep.run, /docker push "\$amd64_ref"/);
  assert.match(publishStep.run, /docker push "\$arm64_ref"/);

  const manifestStep = steps.find((step: { name?: string }) => step.name === "Publish audited multi-platform manifests");
  assert.ok(manifestStep);
  assert.match(manifestStep.run, /docker buildx imagetools create/);
  assert.match(manifestStep.run, /\$\{\{ steps\.publish-platforms\.outputs\.amd64_digest_ref \}\}/);
  assert.match(manifestStep.run, /\$\{\{ steps\.publish-platforms\.outputs\.arm64_digest_ref \}\}/);
});
