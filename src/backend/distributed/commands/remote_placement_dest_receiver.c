#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <arpa/inet.h> /* for htons */
#include <netinet/in.h> /* for htons */
#include <string.h>

#include "access/htup_details.h"
#include "access/htup.h"
#include "access/sdir.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/memutils.h"

static void RemotePlacementDestReceiverStartup(DestReceiver *copyDest, int operation,
										 TupleDesc inputTupleDesc);
static bool RemotePlacementDestReceiverReceive(TupleTableSlot *slot,
										 DestReceiver *copyDest);
static void RemotePlacementDestReceiverShutdown(DestReceiver *destReceiver);
static void RemotePlacementDestReceiverDestroy(DestReceiver *destReceiver);

static void
RemotePlacementDestReceiverStartup(DestReceiver *copyDest, int operation,
										 TupleDesc inputTupleDesc)
{

}


static bool
RemotePlacementDestReceiverReceive(TupleTableSlot *slot,
										 DestReceiver *copyDest)
{

}


static void
RemotePlacementDestReceiverShutdown(DestReceiver *destReceiver)
{

}


static void
RemotePlacementDestReceiverDestroy(DestReceiver *destReceiver)
{
    
}
