..
   This program is copyright 2026 Percona LLC and/or its affiliates.

   THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
   WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.

   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License as published by the Free Software
   Foundation, version 2.

   You should have received a copy of the GNU General Public License, version 2
   along with this program; if not, see <https://www.gnu.org/licenses/>.

#####################################
pt-mongodb-defrag
#####################################

``pt-mongodb-defrag`` is a Go CLI for manual sharded-collection defragmentation on MongoDB.

The tool targets MongoDB 7.0 and newer, uses the official MongoDB Go driver, and warns when it connects to older server versions.

MongoDB's built-in defragmentation documentation is at
`Defragment Sharded Collections <https://www.mongodb.com/docs/manual/core/defragment-sharded-collections/>`_.

--------

Operations
============

The tool provides two distinct operations:

Defragmentation pipeline (default)
-----------------------------------

A multi-phase batch process that measures, merges, and splits chunks across a collection.

1. **Phase 1** — Calculate and store current chunk sizes.
2. **Phase 2** — Merge adjacent chunks where the merged size stays below the configured chunk-size limit. If enabled, it can move one chunk to the adjacent shard first so the merge becomes possible.
3. **Phase 3** — Re-check ``jumbo`` chunks, clear stale jumbo flags, and attempt to split chunks that are still too large.
4. **Phase 4** — Split oversized chunks that are not marked ``jumbo``.

Prevent AutoMerger (``-prevent-automerge``)
--------------------------------------------

A targeted single-chunk operation that prevents MongoDB 7.0+'s AutoMerger from re-merging a specific chunk by splitting it and moving one half to a different shard. The two halves can no longer be auto-merged because they no longer reside on the same shard.

This is useful when:

- An oversized chunk has been identified (e.g., via ``config.chunks`` or monitoring)
- The defrag pipeline's splits are being undone by the AutoMerger merging the resulting chunks back together
- You need to manually redistribute a chunk across shards while keeping the AutoMerger enabled for the rest of the collection

Workflow:

1. Locate the problematic chunk's ``_id`` in ``config.chunks``
2. Choose a split point (a shard key value within the chunk)
3. Choose a target shard that differs from the chunk's current shard
4. Run with ``-prevent-automerge``

The tool:

1. Disables the AutoMerger for the collection
2. Waits for any in-flight AutoMerger cycle to drain (respects the cluster's ``autoMergerIntervalSecs``)
3. Splits the chunk at the query boundary
4. Moves the upper half to the target shard with ``forceJumbo``
5. Re-enables the AutoMerger

**Note:** This operation requires at least two shards in the cluster. It does not support hashed shard keys. If the chunk is marked ``jumbo``, run the defrag pipeline (phases 3/4) first.

--------

Usage
=====

Defragmentation
----------------

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos1:27017/?replicaSet=rs0" \
     -namespace "app.orders" \
     -chunk-size 128M \
     -plan-out /tmp/orders-defrag-plan.json \
     -split-merge-sleep 750ms \
     -sleep 750ms

Dry run:

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos1:27017" \
     -namespace "app.orders" \
     -dry-run

Run specific phases only:

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos1:27017" \
     -namespace "app.orders" \
     -phases 3,4

Prevent AutoMerger
-------------------

Using a JSON query to identify the split point:

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos1:27017" \
     -prevent-automerge \
     -namespace "app.orders" \
     -chunk-id "app.orders-chunk_47" \
     -chunk-query '{"sk": 15000}' \
     -target-shard "shard02"

Using a simpler key=value format (for single-field shard keys):

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos1:27017" \
     -prevent-automerge \
     -namespace "app.orders" \
     -chunk-id "app.orders-chunk_47" \
     -chunk-query "sk=15000" \
     -target-shard "shard02"

--------

Build and test
===============

.. code-block:: bash

   make test
   make build

--------

Generate test data
====================

Generate fragmented test data against an ``mlaunch`` sharded cluster:

.. code-block:: bash

   ./scripts/generate-defrag-test-data.sh

Generate the same dataset but also mark the oversized chunk as ``jumbo`` so phase 3 has a deterministic test case:

.. code-block:: bash

   ./scripts/generate-defrag-test-data.sh --jumbo

Remove the generated test collection and database from the sharded cluster:

.. code-block:: bash

   ./scripts/generate-defrag-test-data.sh --cleanup

--------

Flags reference
================

Defragmentation flags (default mode)
--------------------------------------

======================== ================ =========================================================
Flag                     Default          Description
======================== ================ =========================================================
``-uri``                 (required)       MongoDB connection URI for a mongos router
``-namespace``           (required)       Target namespace in the form ``database.collection``
``-chunk-size``           cluster default Target max chunk size, e.g. ``128M``, ``1G``, or raw bytes
``-sleep``                ``500ms``       Delay between metadata-changing operations (``moveRange``, ``clearJumboFlag``)
``-split-merge-sleep``    ``500ms``       Minimum delay between ``split`` and ``mergeChunks`` commands
``-dry-run``              ``false``       Print planned actions without changing the cluster
``-allow-moves``          ``true``        Allow ``moveRange`` before merging adjacent chunks on different shards
``-allow-zoned-moves``    ``false``       Allow chunk moves on collections with configured zones
``-max-merge-passes``     ``1000``        Maximum merge planning passes
``-plan-out``             ``""``          Path to write the phase-1 sizing snapshot as JSON
``-phases``               ``"1,2,3,4"``  Comma-separated phases to run
``-quiet``                ``false``       Reduce log volume
``-version``              ``false``       Print version information and exit
======================== ================ =========================================================

Prevent-automerge flags (``-prevent-automerge`` mode)
--------------------------------------------------------

======================== ============= ===========================================================================
Flag                     Required      Description
======================== ============= ===========================================================================
``-uri``                 yes           MongoDB connection URI for a mongos router
``-namespace``           yes           Target namespace in the form ``database.collection``
``-chunk-id``            yes           Chunk ``_id`` from ``config.chunks``
``-chunk-query``         yes           Split point query. Accepts JSON (``{"sk": 15000}``) or key=value (``sk=15000``) format. For compound shard keys, use comma-separated pairs (``a=1000,b=abc``)
``-target-shard``        yes           Target shard for the upper half of the split chunk (must differ from the chunk's current shard)
``-auto-approve``        no            Skip the confirmation prompt
======================== ============= ===========================================================================

--------

Minimum required privileges
============================

The tool connects to a ``mongos`` and requires the following privileges on the ``admin`` and ``config`` databases.

Defragmentation pipeline
-------------------------

=============== ========================================================== =====================================
Action          Resource                                                   Used by
=============== ========================================================== =====================================
``find``        ``config.collections``, ``config.chunks``,                  Loading metadata, chunk lists,
                ``config.settings``, ``config.tags``                        chunk size settings, zone checks
``dataSize``    target database                                            Phase 1 — measuring chunk sizes
``splitChunk``  target collection                                          Phases 3/4 — splitting oversized chunks
``moveChunk``   target collection                                          Phase 2 — moving chunks before merges
``mergeChunks`` target collection                                          Phase 2 — merging adjacent chunks
``clearJumboFlag`` target collection                                       Phase 3 — clearing stale jumbo flags
=============== ========================================================== =====================================

Additional privileges for ``-prevent-automerge``
--------------------------------------------------

===================== ================================ ============================================
Action                Resource                         Used by
===================== ================================ ============================================
``configureCollectionBalancing``                       Disabling and re-enabling the AutoMerger
                      target database/collection
``getParameter``       cluster                         Reading the cluster's ``autoMergerIntervalSecs``
``splitChunk``         target collection               Splitting the chunk at the query boundary
``moveChunk``          target collection               Moving the upper half to the target shard
``find``               ``config.chunks``               Looking up the chunk by ``_id``
===================== ================================ ============================================

--------

Notes
======

- **Always connect to a ``mongos``**, not directly to a shard member.
- On MongoDB 7.0 and later, adjacent chunks are merged automatically and MongoDB documentation says manual defragmentation is typically unnecessary. Use this tool for exceptional cases such as post-cleanup compaction, older operational patterns, or when you need tighter control over pacing and visibility.
- Prefer running the defragmentation pipeline during a shard balancing window. MongoDB documents that defragmentation can trigger many metadata updates and increase CRUD latency while those updates propagate.
- Cross-shard ``moveRange`` can fail after copying data if MongoDB hits a donor-side orphan cleanup time limit. If that happens, let the cluster settle and retry, or rerun with ``-allow-moves=false`` to limit phase 2 to same-shard merges.
- Chunk merges erase placement history for the merged ranges. MongoDB documents that snapshot reads and indirectly some transactions can temporarily fail with stale chunk history errors while the new metadata settles.
- For hashed shard keys the defragmentation pipeline uses bounds-based splitting.
- For ranged shard keys the defragmentation pipeline uses the ``split`` command with ``find`` to request a median split for the chunk.
- The ``-prevent-automerge`` mode does **not** support hashed shard keys.
- The ``-prevent-automerge`` mode uses ``forceJumbo`` on ``moveRange`` to bypass MongoDB's document-count move restriction.
- The ``-prevent-automerge`` mode aborts if the chunk is marked ``jumbo``.
- Zoned collections: when using ``-prevent-automerge``, the tool warns if the collection has zones but does not block the operation — ensure the target shard is consistent with your zone configuration or the balancer may move the chunk back.
