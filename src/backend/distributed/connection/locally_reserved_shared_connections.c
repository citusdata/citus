/*-------------------------------------------------------------------------
 *
 * locally_reserved_shared_connections.c
 *
 *   Keeps track of the number of reserved connections to remote nodes
 *   for this backend. The primary goal is to complement the logic
 *   implemented in shared_connections.c which aims to prevent excessive
 *   number of connections (typically > max_connections) to any worker node.
 *   With this locally reserved connection stats, we enforce the same
 *   constraints considering these locally reserved shared connections.
 *
 *   To be more precise, shared connection stats are incremented only with two
 *   operations: (a) Establishing a connection to a remote node
 *               (b) Reserving connections, the logic that this
 *                   file implements.
 *
 *   Finally, as the name already implies, once a node has reserved a  shared
 *   connection, it is guaranteed to have the right to establish a connection
 *   to the given remote node when needed.
 *
 *   For COPY command, we use this fact to reserve connections to the remote nodes
 *   in the same order as the adaptive executor in order to prevent any resource
 *   starvations. We need to do this because COPY establishes connections when it
 *   recieves a tuple that targets a remote node. This is a valuable optimization
 *   to prevent unnecessary connection establishments, which are pretty expensive.
 *   Instead, COPY command can reserve connections upfront, and utilize them when
 *   they are actually needed.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "distributed/listutils.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_manager.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM < PG_VERSION_13
#include "utils/hashutils.h"
#else
#include "common/hashfn.h"
#endif


#define RESERVED_CONNECTION_COLUMNS 4


/* session specific hash map*/
static HTAB *SessionLocalReservedConnections = NULL;


/*
 * Hash key for connection reservations
 */
typedef struct ReservedConnectionHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	Oid databaseOid;
	Oid userId;
} ReservedConnectionHashKey;

/*
 * Hash entry for per worker information. The rules are as follows:
 *  - If there is no entry in the hash, we can make a reservation.
 *  - If usedReservation is false, we have a reservation that we can use.
 *  - If usedReservation is true, we used the reservation and cannot make more reservations.
 */
typedef struct ReservedConnectionHashEntry
{
	ReservedConnectionHashKey key;

	bool usedReservation;
} ReservedConnectionHashEntry;


static void StoreAllReservedConnections(Tuplestorestate *tupleStore,
										TupleDesc tupleDescriptor);
static ReservedConnectionHashEntry * AllocateOrGetReservedConectionEntry(char *hostName,
																		 int nodePort, Oid
																		 userId, Oid
																		 databaseOid,
																		 bool *found);
static void EnsureConnectionPossibilityForNodeList(List *nodeList);
static uint32 LocalConnectionReserveHashHash(const void *key, Size keysize);
static int LocalConnectionReserveHashCompare(const void *a, const void *b, Size keysize);


PG_FUNCTION_INFO_V1(citus_reserved_connection_stats);

/*
 * citus_reserved_connection_stats returns all the avaliable information about all
 * the reserved connections. This function is used mostly for testing.
 */
Datum
citus_reserved_connection_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;

	CheckCitusVersion(ERROR);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllReservedConnections(tupleStore, tupleDescriptor);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * StoreAllReservedConnections gets connections established from the current node
 * and inserts them into the given tuplestore.
 *
 * We don't need to enforce any access privileges as the number of backends
 * on any node is already visible on pg_stat_activity to all users.
 */
static void
StoreAllReservedConnections(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[RESERVED_CONNECTION_COLUMNS];
	bool isNulls[RESERVED_CONNECTION_COLUMNS];

	HASH_SEQ_STATUS status;
	ReservedConnectionHashEntry *connectionEntry = NULL;

	hash_seq_init(&status, SessionLocalReservedConnections);
	while ((connectionEntry =
				(ReservedConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		/* get ready for the next tuple */
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		char *databaseName = get_database_name(connectionEntry->key.databaseOid);
		if (databaseName == NULL)
		{
			/* database might have been dropped */
			continue;
		}

		values[0] = PointerGetDatum(cstring_to_text(connectionEntry->key.hostname));
		values[1] = Int32GetDatum(connectionEntry->key.port);
		values[2] = PointerGetDatum(cstring_to_text(databaseName));
		values[3] = BoolGetDatum(connectionEntry->usedReservation);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
}


/*
 * InitializeLocallyReservedSharedConnections initializes the hashmap in
 * ConnectionContext.
 */
void
InitializeLocallyReservedSharedConnections(void)
{
	HASHCTL reservedConnectionInfo;

	memset(&reservedConnectionInfo, 0, sizeof(reservedConnectionInfo));
	reservedConnectionInfo.keysize = sizeof(ReservedConnectionHashKey);
	reservedConnectionInfo.entrysize = sizeof(ReservedConnectionHashEntry);

	/*
	 * ConnectionContext is the session local memory context that is used for
	 * tracking remote connections.
	 */
	reservedConnectionInfo.hcxt = ConnectionContext;

	reservedConnectionInfo.hash = LocalConnectionReserveHashHash;
	reservedConnectionInfo.match = LocalConnectionReserveHashCompare;

	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	SessionLocalReservedConnections =
		hash_create("citus session level reserved connectios (host,port,database,user)",
					64, &reservedConnectionInfo, hashFlags);
}


/*
 *  CanUseReservedConnection returns true if we have already reserved at least
 *  one shared connection in this session that is not used.
 */
bool
CanUseReservedConnection(const char *hostName, int nodePort, Oid userId,
						 Oid databaseOid)
{
	ReservedConnectionHashKey key;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.userId = userId;
	key.port = nodePort;
	key.databaseOid = databaseOid;

	bool found = false;
	ReservedConnectionHashEntry *entry =
		(ReservedConnectionHashEntry *) hash_search(SessionLocalReservedConnections, &key,
													HASH_FIND, &found);

	if (!found || !entry)
	{
		return false;
	}

	return !entry->usedReservation;
}


/*
 * DeallocateReservedConnections is responsible for two things. First, if the operation
 * has reserved a connection but not used, it gives back the connection back to the
 * shared memory pool. Second, for all cases, it deallocates the session local entry from
 * the hash.
 */
void
DeallocateReservedConnections(void)
{
	HASH_SEQ_STATUS status;
	ReservedConnectionHashEntry *entry;

	hash_seq_init(&status, SessionLocalReservedConnections);
	while ((entry = (ReservedConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (!entry->usedReservation)
		{
			/*
			 * We have not used this reservation, make sure to clean-up from
			 * the shared memory as well.
			 */
			DecrementSharedConnectionCounter(entry->key.hostname, entry->key.port);

			/* for completeness, set it to true */
			entry->usedReservation = true;
		}

		/*
		 * We cleaned up all the entries because we may not need reserved connections
		 * in the next iteration.
		 */
		bool found = false;
		hash_search(SessionLocalReservedConnections, entry, HASH_REMOVE, &found);
		Assert(found);
	}
}


/*
 * MarkReservedConnectionUsed sets the local hash that the reservation is used.
 */
void
MarkReservedConnectionUsed(const char *hostName, int nodePort, Oid userId,
						   Oid databaseOid)
{
	ReservedConnectionHashKey key;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.userId = userId;
	key.port = nodePort;
	key.databaseOid = databaseOid;

	bool found = false;
	ReservedConnectionHashEntry *entry =
		(ReservedConnectionHashEntry *) hash_search(
			SessionLocalReservedConnections, &key, HASH_FIND, &found);

	if (!found)
	{
		ereport(ERROR, (errmsg("BUG: untracked reserved connection"),
						errhint("Set citus.max_shared_pool_size TO -1 to "
								"disable reserved connection counters")));
	}

	/* a reservation can only be used once */
	Assert(!entry->usedReservation);

	entry->usedReservation = true;
}


/*
 * EnsureConnectionPossibilityForPrimaryNodes is a wrapper around
 * EnsureConnectionPossibilityForNodeList.
 */
void
EnsureConnectionPossibilityForPrimaryNodes(void)
{
	/*
	 * By using NoLock there is a tiny risk of that we miss to reserve a
	 * connection for a concurrently added node. However, that doesn't
	 * seem to cause any problems as none of the placements that we are
	 * going to access would be on the new node.
	 */
	List *primaryNodeList = ActivePrimaryNodeList(NoLock);

	EnsureConnectionPossibilityForNodeList(primaryNodeList);
}


/*
 * EnsureConnectionPossibilityForNodeList reserves a shared connection
 * counter per node in the nodeList unless:
 *  - Reservation is needed (see IsReservationPossible())
 *  - there is at least one connection to the node so that we are guranteed
 *    to get a connection
 *  - An earlier call already reserved a connection (e.g., we allow only a
 *    single reservation per backend)
 */
static void
EnsureConnectionPossibilityForNodeList(List *nodeList)
{
	if (!IsReservationPossible())
	{
		return;
	}

	/*
	 * We sort the workerList because adaptive connection management
	 * (e.g., OPTIONAL_CONNECTION) requires any concurrent executions
	 * to wait for the connections in the same order to prevent any
	 * starvation. If we don't sort, we might end up with:
	 *      Execution 1: Get connection for worker 1, wait for worker 2
	 *      Execution 2: Get connection for worker 2, wait for worker 1
	 *
	 *  and, none could proceed. Instead, we enforce every execution establish
	 *  the required connections to workers in the same order.
	 */
	nodeList = SortList(nodeList, CompareWorkerNodes);

	char *databaseName = get_database_name(MyDatabaseId);
	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, nodeList)
	{
		if (ConnectionAvailableToNode(workerNode->workerName, workerNode->workerPort,
									  userName, databaseName) != NULL)
		{
			/*
			 * The same user has already an active connection for the node. It
			 * means that the execution can use the same connection, so reservation
			 * is not necessary.
			 */
			continue;
		}

		/*
		 * We are trying to be defensive here by ensuring that the required hash
		 * table entry can be allocated. The main goal is that we don't want to be
		 * in a situation where shared connection counter is incremented but not
		 * the local reserved counter due to out-of-memory.
		 *
		 * Note that shared connection stats operate on the shared memory, and we
		 * pre-allocate all the necessary memory. In other words, it would never
		 * throw out of memory error.
		 */
		bool found = false;
		ReservedConnectionHashEntry *hashEntry =
			AllocateOrGetReservedConectionEntry(workerNode->workerName,
												workerNode->workerPort,
												userId, MyDatabaseId, &found);

		if (found)
		{
			/*
			 * We have already reserved a connection for this user and database
			 * on the worker. We only allow a single reservation per
			 * transaction block. The reason is that the earlier command (either in
			 * a transaction block or a function call triggered by a single command)
			 * was able to reserve or establish a connection. That connection is
			 * guranteed to be avaliable for us.
			 */
			continue;
		}

		/*
		 * Increment the shared counter, we may need to wait if there are
		 * no space left.
		 */
		WaitLoopForSharedConnection(workerNode->workerName, workerNode->workerPort);

		/* locally mark that we have one connection reserved */
		hashEntry->usedReservation = false;
	}
}


/*
 * IsReservationPossible returns true if the state of the current
 * session is eligible for shared connection reservation.
 */
bool
IsReservationPossible(void)
{
	if (GetMaxSharedPoolSize() == DISABLE_CONNECTION_THROTTLING)
	{
		/* connection throttling disabled */
		return false;
	}

	if (UseConnectionPerPlacement())
	{
		/*
		 * For this case, we are not enforcing adaptive
		 * connection management anyway.
		 */
		return false;
	}

	if (SessionLocalReservedConnections == NULL)
	{
		/*
		 * This is unexpected as SessionLocalReservedConnections hash table is
		 * created at startup. Still, let's be defensive.
		 */
		return false;
	}

	return true;
}


/*
 * AllocateReservedConectionEntry allocates the required entry in the hash
 * map by HASH_ENTER. The function throws an error if it cannot allocate
 * the entry.
 */
static ReservedConnectionHashEntry *
AllocateOrGetReservedConectionEntry(char *hostName, int nodePort, Oid userId,
									Oid databaseOid, bool *found)
{
	ReservedConnectionHashKey key;

	*found = false;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.userId = userId;
	key.port = nodePort;
	key.databaseOid = databaseOid;

	/*
	 * Entering a new entry with HASH_ENTER flag is enough as it would
	 * throw out-of-memory error as it internally does palloc.
	 */
	ReservedConnectionHashEntry *entry =
		(ReservedConnectionHashEntry *) hash_search(SessionLocalReservedConnections,
													&key, HASH_ENTER, found);

	if (!*found)
	{
		/*
		 * Until we reserve connection in the shared memory, we treat
		 * as if have used the reservation.
		 */
		entry->usedReservation = true;
	}

	return entry;
}


/*
 * LocalConnectionReserveHashHash is a utilty function to calculate hash of
 * ReservedConnectionHashKey.
 */
static uint32
LocalConnectionReserveHashHash(const void *key, Size keysize)
{
	ReservedConnectionHashKey *entry = (ReservedConnectionHashKey *) key;

	uint32 hash = string_hash(entry->hostname, MAX_NODE_LENGTH);
	hash = hash_combine(hash, hash_uint32(entry->userId));
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, hash_uint32(entry->databaseOid));

	return hash;
}


/*
 * LocalConnectionReserveHashCompare is a utilty function to compare
 * ReservedConnectionHashKeys.
 */
static int
LocalConnectionReserveHashCompare(const void *a, const void *b, Size keysize)
{
	ReservedConnectionHashKey *ca = (ReservedConnectionHashKey *) a;
	ReservedConnectionHashKey *cb = (ReservedConnectionHashKey *) b;

	if (ca->port != cb->port ||
		ca->databaseOid != cb->databaseOid ||
		ca->userId != cb->userId ||
		strncmp(ca->hostname, cb->hostname, MAX_NODE_LENGTH) != 0)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}
