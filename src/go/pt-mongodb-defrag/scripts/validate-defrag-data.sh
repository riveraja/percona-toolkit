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

# validate-defrag-data.sh
# Validates data integrity before and after running pt-mongodb-defrag.
# Generates a validation report that can be compared before/after defrag.
#
# Usage:
#   ./scripts/validate-defrag-data.sh --uri mongodb://mongos:27017 --namespace db.collection --output /tmp/validation-before.json
#   # Run pt-mongodb-defrag
#   ./scripts/validate-defrag-data.sh --uri mongodb://mongos:27017 --namespace db.collection --output /tmp/validation-after.json
#   # Compare the two reports
#   diff /tmp/validation-before.json /tmp/validation-after.json

set -euo pipefail

uri=''
db_name=''
coll_name=''
output_file=''
mode='before'
sample_size=100

usage() {
  cat <<'EOF'
Usage:
  ./scripts/validate-defrag-data.sh [flags]

Flags:
  --uri <mongodb-uri>       MongoDB connection URI (required)
  --namespace <db.coll>      Target namespace in the form database.collection (required)
  --output <file>            Output file for validation report (required)
  --mode <before|after>      Validation mode (default: before)
  --sample-size <n>          Number of documents to sample for detailed validation (default: 100)
  --help                     Show this help

Examples:
  # Before defrag
  ./scripts/validate-defrag-data.sh \
    --uri mongodb://admin:admin@mongos:27017 \
    --namespace pt_defrag.orders \
    --output /tmp/validation-before.json

  # After defrag
  ./scripts/validate-defrag-data.sh \
    --uri mongodb://admin:admin@mongos:27017 \
    --namespace pt_defrag.orders \
    --output /tmp/validation-after.json \
    --mode after

  # Compare results
  jq . /tmp/validation-before.json
  jq . /tmp/validation-after.json
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uri)
      uri="$2"
      shift 2
      ;;
    --namespace)
      namespace="$2"
      shift 2
      ;;
    --output)
      output_file="$2"
      shift 2
      ;;
    --mode)
      mode="$2"
      shift 2
      ;;
    --sample-size)
      sample_size="$2"
      shift 2
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

# Validate required arguments
if [[ -z "$uri" ]]; then
  echo "ERROR: --uri is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "${namespace:-}" ]]; then
  echo "ERROR: --namespace is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$output_file" ]]; then
  echo "ERROR: --output is required" >&2
  usage >&2
  exit 1
fi

# Parse namespace
db_name=$(echo "$namespace" | cut -d. -f1)
coll_name=$(echo "$namespace" | cut -d. -f2)

if [[ -z "$db_name" || -z "$coll_name" ]]; then
  echo "ERROR: invalid namespace '$namespace', expected database.collection" >&2
  exit 1
fi

# Pass configuration as environment variables to mongosh
export PT_VAL_URI="$uri"
export PT_VAL_DB="$db_name"
export PT_VAL_COLLECTION="$coll_name"
export PT_VAL_MODE="$mode"
export PT_VAL_SAMPLE_SIZE="$sample_size"
export PT_VAL_OUTPUT="$output_file"

echo "=== pt-mongodb-defrag Data Validation ==="
echo "Mode: $mode"
echo "Namespace: $namespace"
echo "Output: $output_file"
echo ""

exec mongosh "$uri" --quiet <<'JSEOF'
// ============================================================
// Data Validation Script for pt-mongodb-defrag
// ============================================================

const URI        = PT_VAL_URI;
const DB_NAME    = PT_VAL_DB;
const COLL_NAME  = PT_VAL_COLLECTION;
const MODE       = PT_VAL_MODE;
const SAMPLE_SIZE = parseInt(PT_VAL_SAMPLE_SIZE);
const OUTPUT     = PT_VAL_OUTPUT;

const NS = `${DB_NAME}.${COLL_NAME}`;

// ============================================================
// Helper Functions
// ============================================================

function getShardKey() {
  // Try to get shard key from config
  const collInfo = db.getSiblingDB('config').collections.findOne({ _id: NS });
  if (collInfo && collInfo.key) {
    return collInfo.key;
  }
  return null;
}

function countDocuments(dbName, collName) {
  return db.getSiblingDB(dbName).getCollection(collName).countDocuments({});
}

function getCollectionStats(dbName, collName) {
  const stats = db.getSiblingDB(dbName).runCommand({ collStats: collName });
  return {
    size: stats.size || 0,
    count: stats.count || 0,
    storageSize: stats.storageSize || 0,
    totalIndexSize: stats.totalIndexSize || 0,
    avgObjSize: stats.avgObjSize || 0,
    nchunks: stats.nchunks || 0,
    nindexes: stats.nindexes || 0
  };
}

function getChunkInfo() {
  const chunks = db.getSiblingDB('config').chunks.find(
    { ns: NS }
  ).sort({ "min": 1 }).toArray();

  let totalChunks = chunks.length;
  let emptyChunks = 0;
  let jumboChunks = 0;

  chunks.forEach(c => {
    if (c.jumbo) jumboChunks++;
  });

  return {
    totalChunks: totalChunks,
    jumboChunks: jumboChunks,
    chunksByShard: chunks.reduce((acc, c) => {
      acc[c.shard] = (acc[c.shard] || 0) + 1;
      return acc;
    }, {})
  };
}

function sampleDocuments(dbName, collName, sampleSize) {
  const coll = db.getSiblingDB(dbName).getCollection(collName);
  const docs = coll.aggregate([
    { $sample: { size: sampleSize } }
  ]).toArray();

  return docs.map(d => ({
    _id: d._id,
    // Include shard key fields if present
    sk: d.sk || null,
    // Include a checksum-like field (first 100 chars of stringified doc)
    checksum: JSON.stringify(d).substring(0, 100)
  }));
}

// ============================================================
// Main Validation Logic
// ============================================================

print(`Starting ${MODE} validation for ${NS}...`);

const validationReport = {
  timestamp: new Date().toISOString(),
  mode: MODE,
  namespace: NS,
  database: DB_NAME,
  collection: COLL_NAME,
  validation: {}
};

// 1. Document Count
print("  [1/5] Counting documents...");
const docCount = countDocuments(DB_NAME, COLL_NAME);
validationReport.documentCount = docCount;
print(`    Document count: ${docCount}`);

// 2. Collection Statistics
print("  [2/5] Getting collection statistics...");
const stats = getCollectionStats(DB_NAME, COLL_NAME);
validationReport.storageStats = stats;
print(`    Storage size: ${stats.storageSize}`);
print(`    Total index size: ${stats.totalIndexSize}`);

// 3. Chunk Information
print("  [3/5] Analyzing chunks...");
const chunkInfo = getChunkInfo();
validationReport.chunkInfo = chunkInfo;
print(`    Total chunks: ${chunkInfo.totalChunks}`);
print(`    Jumbo chunks: ${chunkInfo.jumboChunks}`);

// 4. Sample Documents
print(`  [4/5] Sampling ${SAMPLE_SIZE} documents...`);
const sampledDocs = sampleDocuments(DB_NAME, COLL_NAME, SAMPLE_SIZE);
validationReport.sampledDocuments = sampledDocs;
validationReport.sampleSize = sampledDocs.length;
print(`    Sampled ${sampledDocs.length} documents`);

// 5. Index Information
print("  [5/5] Getting index information...");
const indexes = db.getSiblingDB(DB_NAME).getCollection(COLL_NAME).getIndexes();
validationReport.indexes = indexes.map(idx => ({
  name: idx.name,
  key: idx.key,
  unique: idx.unique || false
}));
print(`    Found ${indexes.length} indexes`);

// Write output
print("");
print(`Writing validation report to: ${OUTPUT}`);
const reportJson = JSON.stringify(validationReport, null, 2);
const outputFile = new java.io.File(OUTPUT);
const parentDir = outputFile.getParentFile();
if (parentDir && !parentDir.exists()) {
  parentDir.mkdirs();
}
const writer = new java.io.FileWriter(outputFile);
writer.write(reportJson);
writer.close();

print("");
print("=== Validation Complete ===");
print(`Report saved to: ${OUTPUT}`);
print("");
print("To compare before/after reports:");
print(`  jq . ${OUTPUT}`);
JSEOF

echo ""
echo "=== Validation script completed ==="
echo "Output: $output_file"
echo ""
echo "To compare before and after results:"
echo "  diff <(jq .documentCount $output_file) <(jq .documentCount /path/to/other/report.json)"
echo ""
