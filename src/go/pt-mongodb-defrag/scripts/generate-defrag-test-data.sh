#!/usr/bin/env bash
set -euo pipefail

uri='mongodb://admin:admin@127.0.0.1:30000/admin'
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
  --uri <mongodb-uri>            Default: mongodb://admin:admin@127.0.0.1:30000/admin
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

PT_DEFRAG_URI_DISPLAY="$uri" \
PT_DEFRAG_DB="$db_name" \
PT_DEFRAG_COLLECTION="$coll_name" \
PT_DEFRAG_CHUNK_COUNT="$chunk_count" \
PT_DEFRAG_CHUNK_SPAN="$chunk_span" \
PT_DEFRAG_DOCS_PER_CHUNK="$docs_per_chunk" \
PT_DEFRAG_DOC_SIZE="$doc_size" \
PT_DEFRAG_DELETE_EVERY="$delete_every" \
PT_DEFRAG_DELETE_PERCENT="$delete_percent" \
PT_DEFRAG_FAT_CHUNK_INDEX="$fat_chunk_index" \
PT_DEFRAG_FAT_CHUNK_DOCS="$fat_chunk_docs" \
PT_DEFRAG_SCATTER="$scatter" \
PT_DEFRAG_SPLIT_MERGE_SLEEP="$split_merge_sleep" \
PT_DEFRAG_JUMBO="$jumbo" \
PT_DEFRAG_CLEANUP="$cleanup" \
PT_DEFRAG_DROP_EXISTING="$drop_existing" \
exec mongosh "$uri" --file ./scripts/generate-defrag-test-data.js
