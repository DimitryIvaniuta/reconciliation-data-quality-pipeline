#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
./scripts/static-verify.py
./gradlew --no-daemon clean check integrationTest bootJar javadoc
python3 -m json.tool postman/Reconciliation-Pipeline.postman_collection.json >/dev/null
python3 -m json.tool postman/Local.postman_environment.json >/dev/null
if command -v docker >/dev/null 2>&1; then APP_SECURITY_API_KEY=verification-only docker compose config >/dev/null; else echo "Docker unavailable; Compose validation skipped" >&2; fi
echo "Verification passed."
