#!/bin/bash

set -e

# Resolve directory of this script (assumes backfiller.js and .env are in same directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment variables from .env file
set -a
source "${SCRIPT_DIR}/.env"
set +a

# Use Node from NVM if available, otherwise fallback to system node
NODE_BIN="${SCRIPT_DIR}/.nvm/versions/node/v18.16.0/bin/node"
if [ ! -x "$NODE_BIN" ]; then
  NODE_BIN="$(command -v node)"
fi

# Execute backfiller.js using resolved Node
"$NODE_BIN" "${SCRIPT_DIR}/backfiller.js"

