#!/bin/bash
set -euo pipefail

# Copy keyfile to a writable location and set strict permissions
cp /etc/mongo-raw/mongod.key /tmp/mongod.key
chmod 400 /tmp/mongod.key

# Execute the container's main command
exec "$@"
