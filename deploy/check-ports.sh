#!/usr/bin/env bash
# Verifies every host port the Vertex Compose stack intends to bind is
# actually free *before* `docker compose up` runs — so you get one clear
# report up front instead of discovering a conflict mid-startup (after
# several other containers have already come up), which is what happened
# during development with both the Jaeger port and the vertex-core port.
#
# Ports are extracted directly from docker-compose.yml's "HOST:CONTAINER"
# mappings, so this script can never drift out of sync with the actual
# compose file — there is exactly one source of truth for the port list.
#
# Usage: ./check-ports.sh          (run from anywhere; the script cds to
#                                    its own directory first)
# Exit code: 0 if every port is free, 1 if any port is already taken,
#            2 if a port's status could not be determined.
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE_FILE="docker-compose.yml"

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "check-ports.sh: $COMPOSE_FILE not found in $(pwd)" >&2
  exit 2
fi

# Extract host-side ports from lines like `"18081:8081"` or `- "1883:1883"`.
ports=$(grep -oE '"[0-9]+:[0-9]+"' "$COMPOSE_FILE" | tr -d '"' | cut -d: -f1 | sort -un)

if [ -z "$ports" ]; then
  echo "No host port mappings found in $COMPOSE_FILE — nothing to check."
  exit 0
fi

port_count=$(echo "$ports" | wc -l | tr -d ' ')
echo "Checking $port_count host port(s) required by the Vertex stack..."
echo

# is_port_free PORT -> prints a status line, returns 0 free / 1 taken / 2 unknown
is_port_free() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    local holder
    holder=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $1" (pid "$2")"}')
    if [ -n "$holder" ]; then
      echo "  ✗ $port  in use by $holder"
      return 1
    fi
    echo "  ✓ $port  free"
    return 0
  fi

  if command -v nc >/dev/null 2>&1; then
    if nc -z -w1 127.0.0.1 "$port" >/dev/null 2>&1; then
      echo "  ✗ $port  in use (run 'lsof -i :$port' for details — install lsof for a fuller report)"
      return 1
    fi
    echo "  ✓ $port  free"
    return 0
  fi

  # Last-resort fallback: bash's own /dev/tcp pseudo-device, no external
  # tools required at all.
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    exec 3>&- 3<&-
    echo "  ✗ $port  in use (neither lsof nor nc available for details)"
    return 1
  fi
  echo "  ✓ $port  free"
  return 0
}

conflict=0
for port in $ports; do
  if ! is_port_free "$port"; then
    conflict=1
  fi
done

echo
if [ "$conflict" -eq 1 ]; then
  cat <<'EOF'
One or more ports above are already taken. Options:
  1. Stop whatever is using them, e.g.:
       docker ps --filter "publish=<PORT>"   # if it's a Docker container
       lsof -i :<PORT>                        # if it's a non-Docker process
  2. Remap the conflicting port(s) in deploy/docker-compose.yml — edit only
     the HOST side of "HOST:CONTAINER" (the left number); leave the
     container-internal side and every VERTEX_*_ADDR value untouched.

See docs/RUNBOOK.md's "port is already allocated" section for the full
troubleshooting playbook.
EOF
  exit 1
fi

echo "All $port_count ports free — safe to run: docker compose up --build"
