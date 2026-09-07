/*-------------------------------------------------------------------------
 *
 * metadata_sync_pool.h
 *    Generic wave-less connection-pool executor for metadata sync.
 *
 * A MetadataSyncPool drives K persistent async libpq connections to a single
 * activated node and runs a make(1)-style ready queue over them: whenever a
 * connection is idle it pulls the next ready task from a pluggable task source,
 * deparses that object's DDL on dispatch, sends it, and reaps the result. Object
 * command strings are NEVER materialized up front -- only object identities are
 * held, and each object's DDL is deparsed on dispatch and freed on reap, so
 * coordinator memory is bounded by the number of in-flight objects (<= K), not by
 * the object count.
 *
 * The dispatch -> wait -> reap connection core in metadata_sync_pool.c is source
 * agnostic: it never touches a ready queue, a scan cursor, or a dependency edge
 * directly. All source-specific behavior is provided through a
 * MetadataSyncTaskSourceOps vtable set on the pool. Two sources are implemented in
 * metadata_sync.c:
 *
 *   edge-gated source (dependency-creation phase)
 *       Tasks are materialized up front from a dependency-ordered ObjectAddress
 *       list plus an edge set. The ready queue is seeded with the in-degree-0
 *       tasks and drained with in-degree gating: completing a task advances its
 *       successors. Used for the small tangled prerequisite DAG (roles, schemas,
 *       types, functions, ...).
 *
 *   streaming-leaf source (shell-table and sequence phases)
 *       Tasks are produced on demand by scanning a catalog relation
 *       (pg_dist_partition for shell tables, pg_dist_object for sequences). The
 *       scanned classes are leaves with no intra-class edges, so there is no HTAB,
 *       no edge set, and no successor bookkeeping: at most one task per connection
 *       is ever live, each object's identity is read straight from the scan
 *       cursor, its DDL is deparsed on dispatch and freed on reap. This is how the
 *       numerous classes (up to ~10M shell tables) stay within a bounded memory
 *       budget -- only object OIDs are ever touched, never a materialized list of
 *       them or of their command strings.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_METADATA_SYNC_POOL_H
#define CITUS_METADATA_SYNC_POOL_H

#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"

#include "distributed/connection_management.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_manager.h"

/*
 * MetadataSyncPoolTask is one object to (re)create on the target node. For an
 * edge-gated source it also carries the in-degree (count of not-yet-created
 * prerequisites that are also tasks) and a successor list; for a streaming-leaf
 * source those stay at their zero-initialized values (in-degree 0, no successors).
 */
typedef struct MetadataSyncPoolTask
{
	ObjectAddress objectAddress;
	int inDegree;
	List *successors;           /* List of MetadataSyncPoolTask * */
	bool dispatched;
	bool done;
} MetadataSyncPoolTask;

typedef struct MetadataSyncPoolConnection
{
	MultiConnection *connection;
	MetadataSyncPoolTask *inFlightTask;
} MetadataSyncPoolConnection;

struct MetadataSyncPool;

/*
 * MetadataSyncTaskSourceOps is the vtable that specializes the generic pool
 * executor for a particular task source. The dispatch -> wait -> reap core calls
 * only these ops; it never inspects a ready queue, scan cursor, or edge set
 * itself.
 *
 *   seed        (optional) called once before the drain loop. The edge-gated
 *               source seeds the ready queue with its in-degree-0 tasks; the
 *               streaming source has nothing to seed (NULL).
 *   pullReady   (required) returns the next task an idle connection should run, or
 *               NULL if none is available right now (ready queue empty / scan
 *               drained).
 *   deparse     (required) produces the DDL command list for a task (NIL if there
 *               is nothing to send, in which case the task is completed inline).
 *   onComplete  (required) records a task as done. The edge-gated source advances
 *               successors' in-degrees; the streaming source counts, flushes
 *               caches, logs progress, and frees the task.
 *   onDrained   (optional) called once when the pool goes idle with no task in
 *               flight. The edge-gated source raises on remaining tasks (a
 *               dependency cycle); the streaming source is done (NULL).
 *   close       (optional) tears down source-owned resources before the
 *               connections are closed. The streaming source ends its catalog
 *               scan; the edge-gated source owns nothing extra (NULL).
 */
typedef struct MetadataSyncTaskSourceOps
{
	void (*seed)(struct MetadataSyncPool *pool);
	MetadataSyncPoolTask *(*pullReady)(struct MetadataSyncPool *pool);
	List *(*deparse)(struct MetadataSyncPool *pool, MetadataSyncPoolTask *task);
	void (*onComplete)(struct MetadataSyncPool *pool, MetadataSyncPoolTask *task);
	void (*onDrained)(struct MetadataSyncPool *pool);
	void (*close)(struct MetadataSyncPool *pool);
} MetadataSyncTaskSourceOps;

typedef struct MetadataSyncPool
{
	MetadataSyncContext *context;
	WorkerNode *workerNode;
	int connectionCount;
	MetadataSyncPoolConnection *connections;    /* array [connectionCount] */
	const MetadataSyncTaskSourceOps *ops;

	/* progress/log label, e.g. "dependency objects", "shell tables" */
	const char *objectLabel;

	/* edge-gated source: tasks materialized up front, drained with in-degree gating */
	HTAB *taskByAddress;
	List *taskList;             /* all MetadataSyncPoolTask *, for iteration/seeding */
	List *readyQueue;           /* MetadataSyncPoolTask * with inDegree 0, not dispatched */
	int64 remainingTasks;

	/* streaming-leaf source: on-demand catalog scan, at most K live tasks */
	Relation streamRelation;
	SysScanDesc streamScan;
	TupleDesc streamTupleDesc;
	bool streamDone;
	Oid (*streamExtractOid)(HeapTuple tuple, TupleDesc tupleDesc);
	List *(*streamBuilder)(Oid objectId);

	int64 completedTasks;
	int64 totalTasks;
	MemoryContext poolContext;      /* long-lived across the phase */
	MemoryContext perObjectContext; /* reset per deparse */
} MetadataSyncPool;

extern MetadataSyncPool * OpenMetadataSyncPool(MetadataSyncContext *context,
											   WorkerNode *workerNode,
											   int connectionCount,
											   const MetadataSyncTaskSourceOps *ops,
											   const char *objectLabel);
extern void RunMetadataSyncPool(MetadataSyncPool *pool);
extern void CloseMetadataSyncPool(MetadataSyncPool *pool);

#endif   /* CITUS_METADATA_SYNC_POOL_H */
