#!/bin/bash
# ══════════════════════════════════════════════════════════════
# SPKN C2 — Local Test Runner
# Run this to test the server locally before pushing to Render
# ══════════════════════════════════════════════════════════════

set -e

echo "══════════════════════════════════════"
echo "  SPKN C2 — Local Test"
echo "══════════════════════════════════════"

# set defaults for local testing
export PANEL_PASSWORD_HASH=${PANEL_PASSWORD_HASH:-$(python3 -c "import hashlib; print(hashlib.sha256(b'test').hexdigest())")}
export AGENT_AUTH_TOKEN=${AGENT_AUTH_TOKEN:-"test-token"}
export PORT=${PORT:-8443}

echo "  Password: test (change in production!)"
echo "  Agent token: $AGENT_AUTH_TOKEN"
echo "  URL: http://localhost:$PORT"
echo ""

# install deps
pip install -r requirements.txt 2>/dev/null

# run
uvicorn main:app --host 0.0.0.0 --port $PORT --reload
