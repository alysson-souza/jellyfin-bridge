#!/usr/bin/env sh
set -eu

image="${1:-jellyfin-bridge:local}"
failures=0
container_id=""
runtime_container_id=""
archive=""
image_archive=""
runtime_config_dir=""

cleanup() {
  if [ -n "$container_id" ]; then
    docker rm "$container_id" >/dev/null 2>&1 || true
  fi
  if [ -n "$runtime_container_id" ]; then
    docker rm -f "$runtime_container_id" >/dev/null 2>&1 || true
  fi
  if [ -n "$archive" ]; then
    rm -f "$archive"
  fi
  if [ -n "$image_archive" ]; then
    rm -f "$image_archive"
  fi
  if [ -n "$runtime_config_dir" ]; then
    rm -rf "$runtime_config_dir"
  fi
}
trap cleanup EXIT

fail() {
  failures=$((failures + 1))
  printf 'not ok - %s\n' "$1"
}

pass() {
  printf 'ok - %s\n' "$1"
}

image_user="$(docker image inspect "$image" --format '{{.Config.User}}')"
image_user_name="${image_user%%:*}"
case "$image_user_name" in
  "" | "0" | "root")
    fail "image declares a non-root runtime user"
    printf '  found user: %s\n' "$image_user"
    ;;
  *)
    pass "image declares a non-root runtime user"
    ;;
esac

if docker run --rm "$image" -e "if (typeof process.getuid === 'function' && process.getuid() === 0) { console.error('runtime uid is 0'); process.exit(1); }"; then
  pass "runtime uid is non-root"
else
  fail "runtime uid is non-root"
fi

container_id="$(docker create "$image")"
archive="$(mktemp "${TMPDIR:-/tmp}/jellyfin-bridge-image.XXXXXX")"
docker export "$container_id" -o "$archive"
docker rm "$container_id" >/dev/null
container_id=""

image_paths="$(tar -tf "$archive" | sed 's#^\./##' | sort)"
image_listing="$(tar -tvf "$archive")"

suid_sgid_paths="$(printf '%s\n' "$image_listing" | awk 'substr($1, 4, 1) ~ /[sS]/ || substr($1, 7, 1) ~ /[sS]/ { print }')"
if [ -z "$suid_sgid_paths" ]; then
  pass "image filesystem has no SUID or SGID paths"
else
  fail "image filesystem has no SUID or SGID paths"
  printf '%s\n' "$suid_sgid_paths" | sed 's/^/  /'
fi

image_archive="$(mktemp "${TMPDIR:-/tmp}/jellyfin-bridge-saved-image.XXXXXX")"
docker save "$image" -o "$image_archive"
file_capability_entries="$(strings "$image_archive" | grep 'security\.capability' || true)"
if [ -z "$file_capability_entries" ]; then
  pass "image layer metadata has no file capabilities"
else
  fail "image layer metadata has no file capabilities"
  printf '%s\n' "$file_capability_entries" | sed 's/^/  /'
fi

node_package_manager_paths=""
for path in \
  usr/local/bin/npm \
  usr/local/bin/npx \
  usr/local/bin/yarn \
  usr/local/bin/yarnpkg \
  usr/local/lib/node_modules/npm
do
  if printf '%s\n' "$image_paths" | grep -Fx "$path" >/dev/null || printf '%s\n' "$image_paths" | grep -F "${path}/" >/dev/null; then
    node_package_manager_paths="${node_package_manager_paths}${path}
"
  fi
done

if [ -z "$node_package_manager_paths" ]; then
  pass "runtime image has no Node package-manager entrypoints"
else
  fail "runtime image has no Node package-manager entrypoints"
  printf '%s' "$node_package_manager_paths" | sed 's/^/  /'
fi

if docker run --rm "$image" -e "const fs = require('fs'); const path = require('path'); const writable = []; function walk(target) { try { fs.accessSync(target, fs.constants.W_OK); writable.push(target); } catch {} const stat = fs.lstatSync(target); if (!stat.isDirectory()) return; for (const entry of fs.readdirSync(target)) walk(path.join(target, entry)); } walk('/app'); if (writable.length) { console.error(writable.join('\n')); process.exit(1); }"; then
  pass "runtime user cannot write application files"
else
  fail "runtime user cannot write application files"
fi

if docker run --rm "$image" -e "const fs = require('fs'); let configWritable = false; try { fs.writeFileSync('/config/audit-probe', 'x'); configWritable = true; } catch {} if (configWritable) { console.error('/config is writable'); process.exit(1); } fs.writeFileSync('/data/audit-probe', 'x'); fs.rmSync('/data/audit-probe');"; then
  pass "runtime user can write /data but not /config"
else
  fail "runtime user can write /data but not /config"
fi

if docker run --rm "$image" -e "const fs = require('fs'); const path = require('path'); const allowedWritablePrefixes = ['/data']; const skipped = new Set(['/dev', '/proc', '/sys']); const unexpected = []; function allowed(target) { return allowedWritablePrefixes.some((prefix) => target === prefix || target.startsWith(prefix + '/')); } function walk(target) { if (skipped.has(target)) return; let stat; try { stat = fs.lstatSync(target); } catch { return; } if (stat.isSymbolicLink()) return; try { fs.accessSync(target, fs.constants.W_OK); if (!allowed(target)) unexpected.push(target); } catch {} if (!stat.isDirectory()) return; let entries; try { entries = fs.readdirSync(target); } catch { return; } for (const entry of entries) walk(path.join(target, entry)); } walk('/'); if (unexpected.length) { console.error(unexpected.join('\n')); process.exit(1); }"; then
  pass "runtime user can write only /data by default"
else
  fail "runtime user can write only /data by default"
fi

forbidden_repo_files=""
while IFS= read -r path; do
  path_basename="${path##*/}"
  forbidden_path=""
  case "$path" in
    .git | .git/* | */.git | */.git/* | \
    app/data | app/data/* | \
    app/src | app/src/* | \
    app/tests | app/tests/* | \
    app/tsconfig.json)
      forbidden_path=1
      ;;
  esac
  case "$path_basename" in
    .env | .env.* | \
    .npmrc | .npmrc.* | \
    config.yaml | config.docker.yaml | *.local.yaml | \
    *.db | *.db-shm | *.db-wal | \
    *.sqlite | *.sqlite-shm | *.sqlite-wal)
      forbidden_path=1
      ;;
  esac
  if [ -n "$forbidden_path" ]; then
      forbidden_repo_files="${forbidden_repo_files}/${path}
"
  fi
done <<EOF
$image_paths
EOF

if [ -z "$forbidden_repo_files" ]; then
  pass "image excludes local secrets, source, tests, and generated state"
else
  fail "image excludes local secrets, source, tests, and generated state"
  printf '%s' "$forbidden_repo_files" | sed 's/^/  /'
fi

if docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges:true "$image" -e "const fs = require('fs'); const Database = require('better-sqlite3'); const db = new Database('/data/audit.db'); db.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY)'); db.close(); fs.rmSync('/data/audit.db');" >/dev/null; then
  pass "image opens SQLite on /data under read-only rootfs with all capabilities dropped"
else
  fail "image opens SQLite on /data under read-only rootfs with all capabilities dropped"
fi

runtime_config_dir="$(mktemp -d "${TMPDIR:-/tmp}/jellyfin-bridge-runtime.XXXXXX")"
cat > "$runtime_config_dir/config.yaml" <<'EOF'
server:
  bind: 0.0.0.0
  port: 8096
  name: Audit Bridge
auth:
  users:
    - name: audit
      passwordHash: hash
upstreams:
  - id: audit
    name: Audit
    url: https://audit.invalid
    token: token
startup:
  validateUpstreams: false
libraries: []
EOF

runtime_container_id="$(docker run -d --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  -v "$runtime_config_dir/config.yaml:/config/config.yaml:ro" \
  -p 127.0.0.1::8096 \
  "$image")"

runtime_url=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  runtime_port="$(docker port "$runtime_container_id" 8096/tcp 2>/dev/null | head -n 1 || true)"
  if [ -n "$runtime_port" ]; then
    runtime_url="http://$runtime_port/System/Ping"
    if node -e "fetch(process.argv[1]).then((response) => process.exit(response.ok ? 0 : 1)).catch(() => process.exit(1))" "$runtime_url"; then
      break
    fi
  fi
  sleep 1
done

health_status=""
for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do
  health_status="$(docker inspect "$runtime_container_id" --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' 2>/dev/null || true)"
  if [ "$health_status" = "healthy" ]; then
    break
  fi
  sleep 5
done

if [ -n "$runtime_url" ] && node -e "fetch(process.argv[1]).then((response) => process.exit(response.ok ? 0 : 1)).catch(() => process.exit(1))" "$runtime_url" && [ "$health_status" = "healthy" ]; then
  pass "image starts, passes healthcheck, and serves /System/Ping under hardened runtime options"
else
  fail "image starts, passes healthcheck, and serves /System/Ping under hardened runtime options"
  printf '  health status: %s\n' "${health_status:-unknown}"
  docker logs "$runtime_container_id" 2>&1 | sed 's/^/  /' || true
fi

if [ "$failures" -ne 0 ]; then
  printf '%s check(s) failed\n' "$failures" >&2
  exit 1
fi
