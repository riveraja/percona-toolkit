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

The tool runs a multi-phase defragmentation pipeline:

1. **Phase 1** — Calculate and store current chunk sizes.
2. **Phase 2** — Merge adjacent chunks where the merged size stays below the configured chunk-size limit. If enabled, it can move one chunk to the adjacent shard first so the merge becomes possible.
3. **Phase 3** — Re-check ``jumbo`` chunks, clear stale jumbo flags, and attempt to split chunks that are still too large.
4. **Phase 4** — Split oversized chunks that are not marked ``jumbo``.

--------

Usage
=====

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

.. note::
   Phases 2–4 automatically calculate chunk sizes when phase 1 is skipped.
   The phase-1 snapshot (written via ``-plan-out``) will be empty in that case.

--------
Build and test
===============

.. code-block:: bash

   # From the repository root:
   cd src/go && go build -o ../../bin/pt-mongodb-defrag ./pt-mongodb-defrag

   # Run all tests:
   cd src/go && go test ./pt-mongodb-defrag/...

   # Build all Go tools using the root build system:
   cd src/go && make build

--------

Docker test environment
========================

The repository includes a ``docker-compose.yml`` that starts a MongoDB 8.0
sharded cluster with authentication for testing pt-mongodb-defrag.

Start the cluster:

.. code-block:: bash

   cd src/go/pt-mongodb-defrag
   docker compose up -d

The ``setup`` service automatically initializes replica sets, adds shards
to the cluster, and creates the ``admin / admin`` administrator user.
The mongos is exposed on ``localhost:30000``.

Populate test data:

.. code-block:: bash

   docker compose exec mongos /tool-scripts/generate-defrag-test-data.sh \
     --uri "mongodb://admin:admin@localhost:27017/admin"

Connect to the cluster with ``mongosh`` from your host:

.. code-block:: bash

   mongosh "mongodb://admin:admin@localhost:30000/admin"

Run the defrag tool against the test cluster:

.. code-block:: bash

   pt-mongodb-defrag -uri "mongodb://admin:admin@localhost:30000/admin" \
     -namespace pt_defrag.orders

Stop and clean everything:

.. code-block:: bash

   docker compose down -v

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

--------

Minimum required privileges
============================

The tool connects to a ``mongos`` and requires the following privileges on the ``admin`` and ``config`` databases.

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

--------

Notes
======

- **Always connect to a ``mongos``**, not directly to a shard member.
- On MongoDB 7.0 and later, adjacent chunks are merged automatically. Use this tool for exceptional cases.
- Prefer running the defragmentation pipeline during a shard balancing window.
- Cross-shard ``moveRange`` can fail if MongoDB hits a donor-side orphan cleanup time limit. Retry after the cluster settles or use ``-allow-moves=false``.
- Chunk merges erase placement history for the merged ranges.
- For hashed shard keys the defragmentation pipeline uses bounds-based splitting.
- For ranged shard keys the defragmentation pipeline uses the ``split`` command with ``find``.
