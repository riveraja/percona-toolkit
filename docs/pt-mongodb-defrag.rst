.. _pt-mongodb-defrag:

=================================
:program:`pt-mongodb-defrag`
=================================

Manually defragments sharded collections on MongoDB 7.0+.

Description
============

A multi-phase batch process that measures, merges, and splits chunks across a collection.

1. **Phase 1** — Calculate and store current chunk sizes.
2. **Phase 2** — Merge adjacent chunks where the merged size stays below the configured chunk-size limit. If enabled, it can move one chunk to the adjacent shard first so the merge becomes possible.
3. **Phase 3** — Re-check ``jumbo`` chunks, clear stale jumbo flags, and attempt to split chunks that are still too large.
4. **Phase 4** — Split oversized chunks that are not marked ``jumbo``.

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

Flags
======

======================== ================ =========================================================
Flag                     Default          Description
======================== ================ =========================================================
``-uri``                 (required)       MongoDB connection URI for a mongos router
``-namespace``           (required)       Target namespace in the form ``database.collection``
``-chunk-size``          cluster default  Target max chunk size, e.g. ``128M``, ``1G``, or raw bytes
``-sleep``               ``500ms``        Delay between metadata-changing operations
``-split-merge-sleep``   ``500ms``        Minimum delay between ``split`` and ``mergeChunks`` commands
``-dry-run``             ``false``        Print planned actions without changing the cluster
``-allow-moves``         ``true``         Allow ``moveRange`` before merging chunks on different shards
``-allow-zoned-moves``   ``false``        Allow chunk moves on collections with configured zones
``-max-merge-passes``    ``1000``         Maximum merge planning passes
``-plan-out``            ``""``           Path to write the phase-1 sizing snapshot as JSON
``-phases``              ``"1,2,3,4"``   Comma-separated phases to run
``-quiet``               ``false``       Reduce log volume
``-version``             ``false``       Print version information and exit
======================== ================ =========================================================

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

Notes
=====

- Always connect to a ``mongos``, not directly to a shard member.
- On MongoDB 7.0+, adjacent chunks are merged automatically. Use this tool for exceptional cases.
- Prefer running during a shard balancing window to reduce metadata-update impact on CRUD latency.
- Cross-shard ``moveRange`` can fail with orphan cleanup timeouts. Retry after the cluster settles or use ``-allow-moves=false``.
