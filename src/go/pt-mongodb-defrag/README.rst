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
     -uri "mongodb://mongos1:27017" \
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

.. note::
   Phases 2–4 automatically calculate chunk sizes when phase 1 is skipped.
   The phase-1 snapshot (written via ``-plan-out``) will be empty in that case.

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
``-metadata-timeout``    ``30s``        Timeout for metadata queries (``config`` DB lookups)
``-timeout``             ``2m``         Timeout for long-running commands (``dataSize``, ``moveRange``, ``merge``, ``split``, etc.)
``-validate``            ``false``        Validate data integrity before and after defrag
``-sample-percent``      ``10``           Percentage of documents to sample for validation (1-100)
``-version``             ``false``       Print version information and exit
======================== ================ =========================================================

--------

Limitations
============

**Single namespace per run.**
The tool accepts exactly one ``-namespace`` (``database.collection``) per invocation.
To defragment multiple collections, run the tool separately for each.

**No resume on interruption.**
All state (chunk metrics, merge progress, current phase position) is held in memory
only. If the tool is interrupted (Ctrl+C, timeout, crash), there is no way to resume
from where it left off. The ``-plan-out`` snapshot is written for review but is never
consumed by the tool itself.

**Silent split failures in Phases 3 and 4.**
If ``split`` fails for a chunk (e.g., the chunk cannot be split further or the split
point is invalid), the tool logs a warning and continues to the next chunk. The
failed chunk remains oversized and is not retried.

**Estimated ``dataSize`` can affect merge accuracy.**
The tool requests estimated sizes first and falls back to exact measurement only
when the estimate falls within 80–120% of the target chunk size or the chunk is
marked ``jumbo``. Chunks whose estimated size falls outside that window may be
merged based on approximate data, and the real combined size could exceed the
target after merging.

**No machine-readable output.**
The only output format is structured log lines. There is no ``--json`` or
``--report-out`` flag for programmatic consumption.

**``--version`` requires ldflags.**
The ``Version``, ``Build``, ``GoVersion``, and ``Commit`` variables are set via
``-ldflags`` during ``make`` builds. If the tool is built directly with
``go build`` without those flags, ``--version`` prints empty fields.

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

Notes
=====

- Always connect to a ``mongos``, not directly to a shard member.
- On MongoDB 7.0+, adjacent chunks are merged automatically. Use this tool for exceptional cases.
- Prefer running during a shard balancing window to reduce metadata-update impact on CRUD latency.
- Cross-shard ``moveRange`` can fail with orphan cleanup timeouts. Retry after the cluster settles or use ``-allow-moves=false``.

--------

Data Validation
==============

Before and after running ``pt-mongodb-defrag``, it is **strongly recommended** to validate
that no data was lost or corrupted during the defragmentation process.

Automated Validation
--------------------

Use the ``-validate`` flag to automatically validate data integrity:

.. code-block:: bash

   pt-mongodb-defrag \
     -uri "mongodb://mongos:27017" \
     -namespace "app.orders" \
     -validate \
     -sample-percent 10

The tool will:

1. **Before defrag:** Record document count, sample document IDs, and collection stats
2. **After defrag:** Verify document count matches, sampled documents still exist, and report storage changes

Example output:

.. code-block:: text

   INFO  running pre-defrag validation...
   INFO  pre-validation: document count 100000
   INFO  pre-validation: sampled documents 10000
   INFO  pre-validation: collection stats map[size:819200000 storageSize:900000000]
   ...
   INFO  === Post-Defrag Validation ===
   INFO  post-validation: document count 100000
   INFO  Validation PASSED: Document count matches
   INFO  Validation PASSED: All sampled documents exist
   INFO  Storage size comparison pre_storage=900000000 post_storage=750000000 reduction=150000000
   INFO  === Validation Complete ===

Manual Validation
-----------------

For additional confidence, run these MongoDB queries manually before and after defrag:

**1. Document Count (MUST match):**

.. code-block:: javascript

   // Total document count
   db.orders.countDocuments()

**2. Storage Statistics (should improve):**

.. code-block:: javascript

   // Collection stats - focus on storageSize, totalIndexSize
   db.orders.stats()

**3. Chunk Distribution:**

.. code-block:: javascript

   // List all chunks and their data distribution
   var chunks = db.getSiblingDB('config').chunks.find(
     { ns: "pt_defrag.orders" }
   ).sort({ "min.sk": 1 }).toArray();

   print("Chunk analysis:");
   chunks.forEach(c => {
     var minKey = c.min.sk;
     var maxKey = c.max.sk;
     var docsInChunk = db.getSiblingDB('pt_defrag').orders.countDocuments({
       sk: { $gte: minKey, $lt: maxKey }
     });
     print(`Chunk ${c._id}: shard=${c.shard}, docs=${docsInChunk}`);
   });

**4. Data Integrity Check:**

.. code-block:: javascript

   // Verify document structure is intact
   db.orders.findOne()

   // Sample documents across the key range
   for (var i = 0; i < 10; i++) {
     var skValue = i * 1000;
     var doc = db.orders.findOne({ sk: skValue });
     if (doc) {
       print(`sk=${skValue}: found doc with idx=${doc.idx}`);
     }
   }

**5. Compare Storage Efficiency:**

.. code-block:: javascript

   // Run and save output BEFORE defrag
   var statsBefore = db.orders.stats();
   print("=== BEFORE DEFRAG ===");
   print("Storage size: " + statsBefore.storageSize);
   print("Total index size: " + statsBefore.totalIndexSize);

   // Run and compare AFTER defrag
   var statsAfter = db.orders.stats();
   print("=== AFTER DEFRAG ===");
   print("Storage size: " + statsAfter.storageSize);
   print("Total index size: " + statsAfter.totalIndexSize);
   print("Size reduction: " + (statsBefore.storageSize - statsAfter.storageSize) + " bytes");

Validation Checklist
--------------------

============ =========== =============== ==============================================
Metric        Before      After           Expected
============ =========== =============== ==============================================
Document count    X            X               Must match exactly
storageSize       X            Y               Y < X (smaller)
totalIndexSize    X            Y               Y ≤ X
Chunk count       X            Y               Y ≤ X (empty chunks removed)
Empty chunks      X            Y               Y = 0 (ideally)
============ =========== =============== ==============================================

--------

Authors
=======

Percona Toolkit Contributors

ABOUT PERCONA TOOLKIT
=====================

This tool is part of Percona Toolkit, a collection of advanced command-line
tools for MySQL developed by Percona.  Percona Toolkit was forked from two
projects in June, 2011: Maatkit and Aspersa.  Those projects were created by
Baron Schwartz and primarily developed by him and Daniel Nichter.  Visit
`http://www.percona.com/software/ <http://www.percona.com/software/>`_ to learn about other free, open-source
software from Percona.

COPYRIGHT, LICENSE, AND WARRANTY
================================

This program is copyright 2026 Percona LLC and/or its affiliates.

THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, version 2.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA  02111-1307  USA.

VERSION
=======

:program:`pt-mongodb-defrag` 3.7.1
