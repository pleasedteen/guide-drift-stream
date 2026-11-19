#!/usr/bin/env bash
# Runs the full test suite (unit + integration) with readable results.
# Expected to finish well under 80 seconds.
set -euo pipefail
cd "$(dirname "$0")"

# Locate Go if it is not on PATH.
if ! command -v go >/dev/null 2>&1; then
  for d in "$HOME/sdk/go/bin" /usr/local/go/bin /usr/lib/go/bin /opt/go/bin; do
    if [ -x "$d/go" ]; then
      export PATH="$d:$PATH"
      break
    fi
  done
fi
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go toolchain not found on PATH" >&2
  exit 1
fi

echo "=========================================================="
echo " observatory test suite"
echo " go: $(go version)"
echo "=========================================================="

echo
echo "--- [1/3] go build ./... ---"
time go build ./...

echo
echo "--- [2/3] unit tests (internal/obs) ---"
go test ./internal/... -count=1 -timeout 70s -v

echo
echo "--- [3/3] integration tests (tests/: SIGKILL recovery + gateway fixture) ---"
go test ./tests/... -count=1 -timeout 70s -v

echo
echo "=========================================================="
echo " ALL TESTS PASSED"
echo "=========================================================="
