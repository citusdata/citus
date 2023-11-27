/*-------------------------------------------------------------------------
 *
 * connection_management.h
 *   Central management of connections and their life-cycle
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONNECTION_MANAGMENT_H
#define CONNECTION_MANAGMENT_H

#include "postgres.h"

#include "pg_config.h"

#include "lib/ilist.h"
#include "portability/instr_time.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "distributed/remote_transaction.h"
#include "distributed/transaction_management.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

/* used for libpq commands that get an error buffer. Postgres docs recommend 256. */
#define ERROR_BUFFER_SIZE 256

/* values with special behavior for authinfo lookup */
#define WILDCARD_NODE_ID 0
#define LOCALHOST_NODE_ID -1

/* application name used for internal connections in Citus */
#define CITUS_APPLICATION_NAME_PREFIX "citus_internal gpid="

/* application name used for internal connections in rebalancer */
#define CITUS_REBALANCER_APPLICATION_NAME_PREFIX "citus_rebalancer gpid="

/* application name used for connections made by run_command_on_* */
#define CITUS_RUN_COMMAND_APPLICATION_NAME_PREFIX "citus_run_command gpid="

/*
 * application name prefix for move/split replication connections.
 *
 * This application_name is set to the subscription name by logical replication
 * workers, so there is no GPID.
 */
#define CITUS_SHARD_TRANSFER_APPLICATION_NAME_PREFIX "citus_shard_"

/* deal with waiteventset errors */
#define WAIT_EVENT_SET_INDEX_NOT_INITIALIZED -1
#define WAIT_EVENT_SET_INDEX_FAILED -2

/*
 * UINT32_MAX is reserved in pg_dist_node, so we can use it safely.
 */
#define LOCAL_NODE_ID UINT32_MAX

/*
 * If you want to connect to the current node use `LocalHostName`, which is a GUC, instead
 * of the hardcoded loopback hostname. Only if you really need the loopback hostname use
 * this define.
 */
#define LOCAL_HOST_NAME "localhost"


/* forward declare, to avoid forcing large headers on everyone */
struct pg_conn; /* target of the PGconn typedef */
struct MemoryContextData;

/*
 * Flags determining connection establishment behaviour.
 */
enum MultiConnectionMode
{
	/* force establishment of a new connection */
	FORCE_NEW_CONNECTION = 1 << 0,

	FOR_DDL = 1 << 1,

	FOR_DML = 1 << 2,

	/*
	 * During COPY we do not want to use a connection that accessed non-co-located
	 * placements. If there is a connection that did not access another placement,
	 * then use it. Otherwise open a new clean connection.
	 */
	REQUIRE_CLEAN_CONNECTION = 1 << 3,

	OUTSIDE_TRANSACTION = 1 << 4,

	/*
	 * All metadata changes should go through the same connection, otherwise
	 * self-deadlocks are possible. That is because the same metadata (e.g.,
	 * metadata includes the distributed table on the workers) can be modified
	 * accross multiple connections.
	 *
	 * With this flag, we guarantee that there is a single metadata connection.
	 * But note that this connection can be used for any other operation.
	 * In other words, this connection is not exclusively reserved for metadata
	 * operations.
	 */
	REQUIRE_METADATA_CONNECTION = 1 << 5,

	/*
	 * Some connections are optional such as when adaptive executor is executing
	 * a multi-shard command and requires the second (or further) connections
	 * per node. In that case, the connection manager may decide not to allow the
	 * connection.
	 */
	OPTIONAL_CONNECTION = 1 << 6,

	/*
	 * When this flag is passed, via connection throttling, the connection
	 * establishments may be suspended until a connection slot is available to
	 * the remote host.
	 */
	WAIT_FOR_CONNECTION = 1 << 7,

	/*
	 * Use the flag to start a connection for streaming replication.
	 * This flag constructs additional libpq connection parameters needed for streaming
	 * replication protocol. It adds 'replication=database' param which instructs
	 * the backend to go into logical replication walsender mode.
	 *  https://www.postgresql.org/docs/current/protocol-replication.html
	 *
	 * This is need to run 'CREATE_REPLICATION_SLOT' command.
	 */
	REQUIRE_REPLICATION_CONNECTION_PARAM = 1 << 8
};


/*
 * This state is used for keeping track of the initialization
 * of the underlying pg_conn struct.
 */
typedef enum MultiConnectionState
{
	MULTI_CONNECTION_INITIAL,
	MULTI_CONNECTION_CONNECTING,
	MULTI_CONNECTION_CONNECTED,
	MULTI_CONNECTION_FAILED,
	MULTI_CONNECTION_LOST,
	MULTI_CONNECTION_TIMED_OUT
} MultiConnectionState;


/*
 * This state is used for keeping track of the initialization
 * of MultiConnection struct, not specifically the underlying
 * pg_conn. The state is useful to determine the action during
 * clean-up of connections.
 */
typedef enum MultiConnectionStructInitializationState
{
	POOL_STATE_NOT_INITIALIZED,
	POOL_STATE_COUNTER_INCREMENTED,
	POOL_STATE_INITIALIZED
} MultiConnectionStructInitializationState;


/* declaring this directly above causes uncrustify to format it badly */
typedef enum MultiConnectionMode MultiConnectionMode;

typedef struct MultiConnection
{
	/* connection details, useful for error messages and such. */
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];

	/* underlying libpq connection */
	struct pg_conn *pgConn;

	/* connection id */
	uint64 connectionId;

	/* state of the connection */
	MultiConnectionState connectionState;

	/* signal that the connection is ready for read/write */
	bool ioReady;

	/* whether to wait for read/write */
	int waitFlags;

	/* force the connection to be closed at the end of the transaction */
	bool forceCloseAtTransactionEnd;

	/* is the connection currently in use, and shouldn't be used by anything else */
	bool claimedExclusively;

	/* is the replication origin session has already been setup for this connection. */
	bool isReplicationOriginSessionSetup;

	/*
	 * Should be used to access/modify metadata. See REQUIRE_METADATA_CONNECTION for
	 * the details.
	 */
	bool useForMetadataOperations;

	/* time connection establishment was started, for timeout and executor stats */
	instr_time connectionEstablishmentStart;
	instr_time connectionEstablishmentEnd;

	/* membership in list of connections in ConnectionHashEntry */
	dlist_node connectionNode;

	/* information about the associated remote transaction */
	RemoteTransaction remoteTransaction;

	/*
	 * membership in list of in-progress transactions and a flag to indicate
	 * that the connection was added to this list
	 */
	dlist_node transactionNode;
	bool transactionInProgress;

	/* list of all placements referenced by this connection */
	dlist_head referencedPlacements;

	/* number of bytes sent to PQputCopyData() since last flush */
	uint64 copyBytesWrittenSinceLastFlush;

	/* replication option */
	bool requiresReplication;

	MultiConnectionStructInitializationState initializationState;
} MultiConnection;


/*
 * Central connection management hash, mapping (host, port, user, database) to
 * a list of connections.
 *
 * This hash is used to keep track of which connections are open to which
 * node. Besides allowing connection reuse, that information is e.g. used to
 * handle closing connections after the end of a transaction.
 */

/* hash key */
typedef struct ConnectionHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];
	bool replicationConnParam;
} ConnectionHashKey;

/* hash entry */
typedef struct ConnectionHashEntry
{
	ConnectionHashKey key;
	dlist_head *connections;

	/* connections list is valid or not */
	bool isValid;
} ConnectionHashEntry;

/* hash entry for cached connection parameters */
typedef struct ConnParamsHashEntry
{
	ConnectionHashKey key;
	bool isValid;
	Index runtimeParamStart;
	char **keywords;
	char **values;
} ConnParamsHashEntry;


/* maximum duration to wait for connection */
extern int NodeConnectionTimeout;

/* maximum number of connections to cache per worker per session */
extern int MaxCachedConnectionsPerWorker;

/* maximum lifetime of connections in miliseconds */
extern int MaxCachedConnectionLifetime;

/* parameters used for outbound connections */
extern char *NodeConninfo;
extern char *LocalHostName;
extern bool checkAtBootPassed;

/* the hash tables are externally accessiable */
extern HTAB *ConnectionHash;
extern HTAB *ConnParamsHash;

/* context for all connection and transaction related memory */
extern struct MemoryContextData *ConnectionContext;


extern void AfterXactConnectionHandling(bool isCommit);
extern void InitializeConnectionManagement(void);

extern char * GetAuthinfo(char *hostname, int32 port, char *user);
extern void InitConnParams(void);
extern void ResetConnParams(void);
extern void InvalidateConnParamsHashEntries(void);
extern void AddConnParam(const char *keyword, const char *value);
extern void GetConnParams(ConnectionHashKey *key, char ***keywords, char ***values,
						  Index *runtimeParamStart, MemoryContext context);
extern const char * GetConnParam(const char *keyword);
extern bool CheckConninfo(const char *conninfo, const char **allowedConninfoKeywords,
						  Size allowedConninfoKeywordsLength, char **errmsg);


/* Low-level connection establishment APIs */
extern MultiConnection * GetNodeConnection(uint32 flags, const char *hostname,
										   int32 port);
extern MultiConnection * StartNodeConnection(uint32 flags, const char *hostname,
											 int32 port);
extern MultiConnection * GetNodeUserDatabaseConnection(uint32 flags, const char *hostname,
													   int32 port, const char *user,
													   const char *database);
extern MultiConnection * GetConnectionForLocalQueriesOutsideTransaction(char *userName);
extern MultiConnection * StartNodeUserDatabaseConnection(uint32 flags,
														 const char *hostname,
														 int32 port,
														 const char *user,
														 const char *database);
extern void RestartConnection(MultiConnection *connection);
extern void CloseAllConnectionsAfterTransaction(void);
extern void CloseNodeConnectionsAfterTransaction(char *nodeName, int nodePort);
extern MultiConnection * ConnectionAvailableToNode(char *hostName, int nodePort,
												   const char *userName,
												   const char *database);
extern void CloseConnection(MultiConnection *connection);
extern void ShutdownAllConnections(void);
extern void ShutdownConnection(MultiConnection *connection);

/* dealing with a connection */
extern void FinishConnectionListEstablishment(List *multiConnectionList);
extern void FinishConnectionEstablishment(MultiConnection *connection);
extern void ForceConnectionCloseAtTransactionEnd(MultiConnection *connection);
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);
extern void MarkConnectionConnected(MultiConnection *connection);

/* waiteventset utilities */
extern int CitusAddWaitEventSetToSet(WaitEventSet *set, uint32 events, pgsocket fd,
									 Latch *latch, void *user_data);

extern bool CitusModifyWaitEvent(WaitEventSet *set, int pos, uint32 events,
								 Latch *latch);

/* time utilities */
extern double MillisecondsPassedSince(instr_time moment);
extern long MillisecondsToTimeout(instr_time start, long msAfterStart);

#endif /* CONNECTION_MANAGMENT_H */
