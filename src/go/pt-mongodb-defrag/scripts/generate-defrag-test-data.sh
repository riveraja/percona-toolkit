#!/usr/bin/env bash
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

# generate-defrag-test-data.sh
# Generates fragmented test data for pt-mongodb-defrag on a sharded MongoDB cluster.
# Requires: mongosh (or mongo), access to a mongos.

set -euo pipefail

uri='mongodb://admin:admin@[IP_ADDRESS]:30000/admin'
db_name='pt_defrag'
coll_name='orders'
chunk_count='237'
chunk_span='1000'
docs_per_chunk='8'
doc_size='8192'
delete_every='3'
delete_percent='75'
fat_chunk_index='47'
fat_chunk_docs='512'
scatter='auto'
split_merge_sleep='500ms'
drop_existing='true'
jumbo='false'
cleanup='false'

usage() {
  cat <<'EOF'
Usage:
  ./scripts/generate-defrag-test-data.sh [flags]

Flags:
  --uri <mongodb-uri>            Default: mongodb://admin:admin@[IP_ADDRESS]:30000/admin
  --db <name>                    Default: pt_defrag
  --collection <name>            Default: orders
  --chunk-count <n>              Initial chunk count to create. Default: 48
  --chunk-span <n>               Key-space width per chunk. Default: 1000
  --docs-per-chunk <n>           Baseline documents per chunk. Default: 8
  --doc-size <bytes>             Approximate payload size per document. Default: 8192
  --delete-every <n>             Delete from every nth chunk after load. Default: 3
  --delete-percent <n>           Percent of docs to delete from sparse chunks. Default: 75
  --fat-chunk-index <n>          Chunk index that stays oversized. Default: 47
  --fat-chunk-docs <n>           Documents to insert into the oversized chunk. Default: 512
  --scatter <auto|always|never>  Move alternating chunks to another shard when available. Default: auto
  --split-merge-sleep <duration> Recommended pt-mongodb-defrag split/merge pacing. Default: 500ms
  --jumbo                        Mark the oversized test chunk as jumbo to exercise phase 3
  --cleanup                      Drop the target collection and database instead of generating data
  --keep-existing                Reuse the target collection instead of dropping it first
  --help                         Show this help

Examples:
  ./scripts/generate-defrag-test-data.sh
  ./scripts/generate-defrag-test-data.sh --db lab --collection fragmented --scatter always
  ./scripts/generate-defrag-test-data.sh --jumbo
  ./scripts/generate-defrag-test-data.sh --cleanup
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uri)
      uri="$2"
      shift 2
      ;;
    --db)
      db_name="$2"
      shift 2
      ;;
    --collection)
      coll_name="$2"
      shift 2
      ;;
    --chunk-count)
      chunk_count="$2"
      shift 2
      ;;
    --chunk-span)
      chunk_span="$2"
      shift 2
      ;;
    --docs-per-chunk)
      docs_per_chunk="$2"
      shift 2
      ;;
    --doc-size)
      doc_size="$2"
      shift 2
      ;;
    --delete-every)
      delete_every="$2"
      shift 2
      ;;
    --delete-percent)
      delete_percent="$2"
      shift 2
      ;;
    --fat-chunk-index)
      fat_chunk_index="$2"
      shift 2
      ;;
    --fat-chunk-docs)
      fat_chunk_docs="$2"
      shift 2
      ;;
    --scatter)
      scatter="$2"
      shift 2
      ;;
    --split-merge-sleep)
      split_merge_sleep="$2"
      shift 2
      ;;
    --jumbo)
      jumbo='true'
      shift
      ;;
    --cleanup)
      cleanup='true'
      shift
      ;;
    --keep-existing)
      drop_existing='false'
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Pass all configuration as environment variables to mongosh, then run
# the embedded JavaScript via stdin (heredoc) instead of an external .js file.
export PT_DEFRAG_URI_DISPLAY="$uri"
export PT_DEFRAG_DB="$db_name"
export PT_DEFRAG_COLLECTION="$coll_name"
export PT_DEFRAG_CHUNK_COUNT="$chunk_count"
export PT_DEFRAG_CHUNK_SPAN="$chunk_span"
export PT_DEFRAG_DOCS_PER_CHUNK="$docs_per_chunk"
export PT_DEFRAG_DOC_SIZE="$doc_size"
export PT_DEFRAG_DELETE_EVERY="$delete_every"
export PT_DEFRAG_DELETE_PERCENT="$delete_percent"
export PT_DEFRAG_FAT_CHUNK_INDEX="$fat_chunk_index"
export PT_DEFRAG_FAT_CHUNK_DOCS="$fat_chunk_docs"
export PT_DEFRAG_SCATTER="$scatter"
export PT_DEFRAG_SPLIT_MERGE_SLEEP="$split_merge_sleep"
export PT_DEFRAG_JUMBO="$jumbo"
export PT_DEFRAG_CLEANUP="$cleanup"
export PT_DEFRAG_DROP_EXISTING="$drop_existing"

exec mongosh "$uri" <<'JSEOF'
// ============================================================
// Generate fragmented test data for pt-mongodb-defrag
// ============================================================

const DB_NAME          = (typeof PT_DEFRAG_DB !== 'undefined')          ? PT_DEFRAG_DB          : 'pt_defrag';
const COLL_NAME        = (typeof PT_DEFRAG_COLLECTION !== 'undefined')  ? PT_DEFRAG_COLLECTION  : 'orders';
const CHUNK_COUNT      = parseInt((typeof PT_DEFRAG_CHUNK_COUNT !== 'undefined') ? PT_DEFRAG_CHUNK_COUNT : '237');
const CHUNK_SPAN       = parseInt((typeof PT_DEFRAG_CHUNK_SPAN !== 'undefined') ? PT_DEFRAG_CHUNK_SPAN : '1000');
const DOCS_PER_CHUNK   = parseInt((typeof PT_DEFRAG_DOCS_PER_CHUNK !== 'undefined') ? PT_DEFRAG_DOCS_PER_CHUNK : '8');
const DOC_SIZE          = parseInt((typeof PT_DEFRAG_DOC_SIZE !== 'undefined') ? PT_DEFRAG_DOC_SIZE : '8192');
const DELETE_EVERY     = parseInt((typeof PT_DEFRAG_DELETE_EVERY !== 'undefined') ? PT_DEFRAG_DELETE_EVERY : '3');
const DELETE_PERCENT   = parseInt((typeof PT_DEFRAG_DELETE_PERCENT !== 'undefined') ? PT_DEFRAG_DELETE_PERCENT : '75');
const FAT_CHUNK_INDEX  = parseInt((typeof PT_DEFRAG_FAT_CHUNK_INDEX !== 'undefined') ? PT_DEFRAG_FAT_CHUNK_INDEX : '47');
const FAT_CHUNK_DOCS   = parseInt((typeof PT_DEFRAG_FAT_CHUNK_DOCS !== 'undefined') ? PT_DEFRAG_FAT_CHUNK_DOCS : '512');
const SCATTER          = (typeof PT_DEFRAG_SCATTER !== 'undefined')     ? PT_DEFRAG_SCATTER     : 'auto';
const DROP_EXISTING    = (typeof PT_DEFRAG_DROP_EXISTING !== 'undefined') ? (PT_DEFRAG_DROP_EXISTING === 'true') : true;
const JUMBO            = (typeof PT_DEFRAG_JUMBO !== 'undefined')       ? (PT_DEFRAG_JUMBO === 'true') : false;
const CLEANUP          = (typeof PT_DEFRAG_CLEANUP !== 'undefined')     ? (PT_DEFRAG_CLEANUP === 'true') : false;
const SPLIT_MERGE_SLEEP = (typeof PT_DEFRAG_SPLIT_MERGE_SLEEP !== 'undefined') ? PT_DEFRAG_SPLIT_MERGE_SLEEP : '500ms';

const NS = `${DB_NAME}.${COLL_NAME}`;
const SHARD_KEY = { sk: 1 };

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------
function pad(val, width) {
  return String(val).padStart(width, '0');
}

function docPayload(size) {
  // Generate a string of approximately `size` bytes as a repeatable pattern
  return 'x'.repeat(Math.max(0, size - 50));
}

function docsForChunk(chunkIdx, count) {
  const base = chunkIdx * CHUNK_SPAN;
  const payload = docPayload(DOC_SIZE);
  const docs = [];
  for (let i = 0; i < count; i++) {
    docs.push({ sk: base + i, idx: chunkIdx, msg: `chunk-${chunkIdx} doc-${i}`, data: payload });
  }
  return docs;
}

function sleep(ms) {
  // mongosh does not expose a synchronous sleep; use a Date-based busy wait.
  const start = new Date();
  while (new Date() - start < ms) {
    // spin
  }
}

function sleepBetween() {
  const ms = parseInt(SPLIT_MERGE_SLEEP);
  if (ms > 0) sleep(ms);
}

// --------------------------------------------------------------------------
// Cleanup mode
// --------------------------------------------------------------------------
if (CLEANUP) {
  print(`\n=== CLEANUP: dropping ${DB_NAME}.${COLL_NAME} ===`);
  const admin = db.getSiblingDB('admin');
  admin.auth('admin', 'admin');
  const coll = db.getSiblingDB(DB_NAME).getCollection(COLL_NAME);
  const dropResult = coll.drop();
  print(`  drop collection: ${JSON.stringify(dropResult)}`);
  // Remove from config.collections and config.chunks
  db.getSiblingDB('config').collections.deleteOne({ _id: NS });
  db.getSiblingDB('config').chunks.deleteMany({ ns: NS });
  print(`  removed from config.collections and config.chunks`);
  // Drop database
  const dbDrop = db.getSiblingDB(DB_NAME).dropDatabase();
  print(`  dropDatabase: ${JSON.stringify(dbDrop)}`);
  print('=== CLEANUP complete ===\n');
  process.exit(0);
}

// --------------------------------------------------------------------------
// Enrich
// --------------------------------------------------------------------------
const shardKeyField = Object.keys(SHARD_KEY)[0];
const admin = db.getSiblingDB('admin');
admin.auth('admin', 'admin');

// Discover shards
const shards = admin.adminCommand({ listShards: 1 }).shards.map(s => s._id);
shards.sort();
print(`\nShards in cluster: ${JSON.stringify(shards)}`);

if (shards.length === 0) {
  print('ERROR: no shards found. Ensure you are connected to a mongos.');
  process.exit(1);
}

// Enable sharding on the DB
print(`\nEnabling sharding on ${DB_NAME}...`);
admin.adminCommand({ enableSharding: DB_NAME });

// Switch to target DB
const targetDb = db.getSiblingDB(DB_NAME);

// Drop collection if requested
if (DROP_EXISTING) {
  print(`Dropping existing collection ${NS}...`);
  targetDb.getCollection(COLL_NAME).drop();
  sleepBetween();
}

// Shard the collection
print(`Sharding collection ${NS} on key ${JSON.stringify(SHARD_KEY)}...`);
admin.adminCommand({ shardCollection: NS, key: SHARD_KEY });

// --------------------------------------------------------------------------
// Phase 1: create many small chunks + one fat chunk
// --------------------------------------------------------------------------
print(`\n=== Phase 1: Creating ${CHUNK_COUNT} chunks ===`);

// Pre-split the collection
const totalSpan = CHUNK_COUNT * CHUNK_SPAN;
for (let i = 1; i < CHUNK_COUNT; i++) {
  const splitPoint = {};
  splitPoint[shardKeyField] = i * CHUNK_SPAN;
  admin.adminCommand({ split: NS, middle: splitPoint });
  sleepBetween();
}

// Distribute chunks across shards (round-robin)
if (shards.length > 1) {
  print('Distributing chunks across shards...');
  const chunkDocs = db.getSiblingDB('config').chunks.find({ ns: NS }).sort({ min: 1 }).toArray();
  for (let i = 0; i < chunkDocs.length; i++) {
    const targetShard = shards[i % shards.length];
    if (chunkDocs[i].shard !== targetShard) {
      try {
        admin.adminCommand({ moveChunk: NS, find: chunkDocs[i].min, to: targetShard });
        sleepBetween();
      } catch (e) {
        print(`  moveChunk[${i}] to ${targetShard}: ${e.message}`);
      }
    }
  }
}

// --------------------------------------------------------------------------
// Phase 2: insert baseline documents
// --------------------------------------------------------------------------
print(`\n=== Phase 2: Inserting baseline documents ===`);

const coll = targetDb.getCollection(COLL_NAME);
let totalDocs = 0;

for (let i = 0; i < CHUNK_COUNT; i++) {
  const count = (i === FAT_CHUNK_INDEX) ? DOCS_PER_CHUNK : DOCS_PER_CHUNK;
  const docs = docsForChunk(i, count);
  coll.insertMany(docs, { ordered: false });
  totalDocs += count;
}
print(`Inserted ${totalDocs} baseline documents`);

// --------------------------------------------------------------------------
// Phase 3: delete from some chunks to create fragmentation / sparse chunks
// --------------------------------------------------------------------------
print(`\n=== Phase 3: Creating fragmentation (deleting from every ${DELETE_EVERY}th chunk) ===`);

for (let i = 0; i < CHUNK_COUNT; i += DELETE_EVERY) {
  if (i === FAT_CHUNK_INDEX) continue; // keep the fat chunk full

  const base = i * CHUNK_SPAN;
  const docsInChunk = coll.countDocuments({ sk: { $gte: base, $lt: base + CHUNK_SPAN } });
  const toDelete = Math.floor(docsInChunk * DELETE_PERCENT / 100);
  if (toDelete > 0) {
    // Delete by finding the first `toDelete` documents in this range
    const idsToDelete = coll.find({ sk: { $gte: base, $lt: base + CHUNK_SPAN } })
      .limit(toDelete)
      .toArray()
      .map(d => d._id);
    coll.deleteMany({ _id: { $in: idsToDelete } });
  }
}
print('Fragmentation created');

// --------------------------------------------------------------------------
// Phase 4: inflate the designated fat chunk
// --------------------------------------------------------------------------
print(`\n=== Phase 4: Inflating fat chunk at index ${FAT_CHUNK_INDEX} ===`);

const fatBase = FAT_CHUNK_INDEX * CHUNK_SPAN;
const fatDocs = docsForChunk(FAT_CHUNK_INDEX, FAT_CHUNK_DOCS);
coll.insertMany(fatDocs, { ordered: false });
print(`Inserted ${FAT_CHUNK_DOCS} extra documents into fat chunk`);

// --------------------------------------------------------------------------
// Phase 5: optionally mark the fat chunk as jumbo
// --------------------------------------------------------------------------
if (JUMBO) {
  print(`\n=== Phase 5: Marking fat chunk as jumbo ===`);
  const jumboMin = {};
  jumboMin[shardKeyField] = fatBase;
  try {
    // Read the chunk document and manually update config.chunks
    const chunkDoc = db.getSiblingDB('config').chunks.findOne({
      ns: NS,
      [`min.${shardKeyField}`]: fatBase
    });
    if (chunkDoc) {
      db.getSiblingDB('config').chunks.updateOne(
        { _id: chunkDoc._id },
        { $set: { jumbo: true } }
      );
      print(`  Marked chunk ${chunkDoc._id} as jumbo`);
    }
  } catch (e) {
    print(`  Failed to mark jumbo: ${e.message}`);
  }
}

// --------------------------------------------------------------------------
// Summary
// --------------------------------------------------------------------------
print(`\n=== Generation complete ===`);
const totalCount = coll.countDocuments();
const chunkList = db.getSiblingDB('config').chunks.find({ ns: NS }).sort({ min: 1 }).toArray();
print(`  Database:        ${DB_NAME}`);
print(`  Collection:      ${COLL_NAME}`);
print(`  Total documents: ${totalCount}`);
print(`  Total chunks:    ${chunkList.length}`);
print(`  Shards:          ${shards.join(', ')}`);
print(`\nRecommended pt-mongodb-defrag invocation:`);
print(`  pt-mongodb-defrag -uri "${PT_DEFRAG_URI_DISPLAY}" -namespace "${NS}" -split-merge-sleep ${SPLIT_MERGE_SLEEP}`);
print(`\n`);
JSEOF
