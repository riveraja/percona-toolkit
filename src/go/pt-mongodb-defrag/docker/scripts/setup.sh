#!/bin/bash
# This program is copyright 2026 Percona LLC and/or its affiliates.
#
# THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 2.
#
# You should have received a copy of the GNU General Public License, version 2
# along with this program; if not, see <https://www.gnu.org/licenses/>.

# setup.sh
# Initializes the pt-mongodb-defrag test cluster.
# Run automatically by docker-compose as the `setup` service entrypoint.
#
# Prerequisite: all mongod and mongos containers are already running.

set -euo pipefail

MAX_TRIES=60
SLEEP_SECS=2

wait_for_node() {
  local host="$1"
  local port="${2:-27017}"
  local tries=0
  printf "Waiting for %s:%s..." "$host" "$port"
  while true; do
    if mongosh --quiet --host "$host" --port "$port" --eval 'db.runCommand({ ping: 1 }).ok' 2>/dev/null | grep -q '^1'; then
      echo " ready"
      return 0
    fi
    tries=$((tries + 1))
    if [ "$tries" -ge "$MAX_TRIES" ]; then
      echo " FAILED after $MAX_TRIES retries"
      return 1
    fi
    printf "."
    sleep "$SLEEP_SECS"
  done
}

safe_mongosh() {
  local host="$1"
  local port="${2:-27017}"
  shift 2
  mongosh --quiet --host "$host" --port "$port" --eval "$*" 2>/dev/null
}

echo "============================================================"
echo " pt-mongodb-defrag test cluster setup"
echo "============================================================"

# ------------------------------------------------------------------
# 1. Config server replica set
# ------------------------------------------------------------------
echo ""
echo "--- Config server replica set ---"
wait_for_node cfg-1 27017
wait_for_node cfg-2 27017
wait_for_node cfg-3 27017

safe_mongosh cfg-1 27017 "
  rs.initiate({
    _id: 'cfg',
    configsvr: true,
    version: 1,
    members: [
      { _id: 0, host: 'cfg-1:27017' },
      { _id: 1, host: 'cfg-2:27017' },
      { _id: 2, host: 'cfg-3:27017' }
    ]
  });
"
echo "  Config replica set initiated"

# ------------------------------------------------------------------
# 2. Shard 1 replica set
# ------------------------------------------------------------------
echo ""
echo "--- Shard 1 replica set (rs1) ---"
wait_for_node rs1-1 27017
wait_for_node rs1-2 27017
wait_for_node rs1-3 27017

safe_mongosh rs1-1 27017 "
  rs.initiate({
    _id: 'rs1',
    version: 1,
    members: [
      { _id: 0, host: 'rs1-1:27017' },
      { _id: 1, host: 'rs1-2:27017' },
      { _id: 2, host: 'rs1-3:27017' }
    ]
  });
"
echo "  Shard 1 replica set initiated"

# ------------------------------------------------------------------
# 3. Shard 2 replica set
# ------------------------------------------------------------------
echo ""
echo "--- Shard 2 replica set (rs2) ---"
wait_for_node rs2-1 27017
wait_for_node rs2-2 27017
wait_for_node rs2-3 27017

safe_mongosh rs2-1 27017 "
  rs.initiate({
    _id: 'rs2',
    version: 1,
    members: [
      { _id: 0, host: 'rs2-1:27017' },
      { _id: 1, host: 'rs2-2:27017' },
      { _id: 2, host: 'rs2-3:27017' }
    ]
  });
"
echo "  Shard 2 replica set initiated"

# ------------------------------------------------------------------
# 4. Wait for mongos, then add shards
# ------------------------------------------------------------------
echo ""
echo "--- Mongos and shard registration ---"
wait_for_node mongos 27017

safe_mongosh mongos 27017 "
  sh.addShard('rs1/rs1-1:27017,rs1-2:27017,rs1-3:27017');
"
echo "  Shard 1 added"

safe_mongosh mongos 27017 "
  sh.addShard('rs2/rs2-1:27017,rs2-2:27017,rs2-3:27017');
"
echo "  Shard 2 added"

# ------------------------------------------------------------------
# 5. Create admin user
# ------------------------------------------------------------------
echo ""
echo "--- Admin user ---"
safe_mongosh mongos 27017 "
  db.getSiblingDB('admin').createUser({
    user: 'admin',
    pwd: 'admin',
    roles: [
      { db: 'admin', role: 'root' },
      { db: 'config', role: '__system' }
    ]
  });
"
echo "  Admin user created (admin / admin)"

# ------------------------------------------------------------------
# 6. Create a database + collection so sharding is primed
# ------------------------------------------------------------------
echo ""
echo "--- Prime sharding ---"
safe_mongosh mongos 27017 "
  sh.enableSharding('test');
  db.getSiblingDB('test').createCollection('init');
  sh.shardCollection('test.init', { _id: 'hashed' });
"
echo "  Sharding primed"

echo ""
echo "============================================================"
echo " Cluster setup complete"
echo " Connect via: mongosh mongodb://admin:admin@localhost:30000/admin"
echo "============================================================"
