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

#include "distributed/transaction_management.h"
#include "distributed/remote_transaction.h"
#include "lib/ilist.h"
#include "pg_config.h"
#include "portability/instr_time.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

/* used for libpq commands that get an error buffer. Postgres docs recommend 256. */
#define ERROR_BUFFER_SIZE 256

/* application name used for internal connections in Citus */
#define CITUS_APPLICATION_NAME "citus"

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
	 * Some connections are optional such as when adaptive executor is executing
	 * a multi-shard command and requires the second (or further) connections
	 * per node. In that case, the connection manager may decide not to allow the
	 * connection.
	 */
	OPTIONAL_CONNECTION = 1 << 5,

	/*
	 * When this flag is passed, via connection throttling, the connection
	 * establishments may be suspended until a connection slot is available to
	 * the remote host.
	 */
	WAIT_FOR_CONNECTION = 1 << 6
};


/*
 * This state is used for keeping track of the initilization
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
 * This state is used for keeping track of the initilization
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

	/* time connection establishment was started, for timeout and executor stats */
	instr_time connectionEstablishmentStart;
	instr_time connectionEstablishmentEnd;

	/* membership in list of list of connections in ConnectionHashEntry */
	dlist_node connectionNode;

	/* information about the associated remote transaction */
	RemoteTransaction remoteTransaction;

	/* membership in list of in-progress transactions */
	dlist_node transactionNode;

	/* list of all placements referenced by this connection */
	dlist_head referencedPlacements;

	/* number of bytes sent to PQputCopyData() since last flush */
	uint64 copyBytesWrittenSinceLastFlush;

	MultiConnectionStructInitializationState initilizationState;
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

/* the hash tables are externally accessiable */
extern HTAB *ConnectionHash;
extern HTAB *ConnParamsHash;

/* context for all connection and transaction related memory */
extern struct MemoryContextData *ConnectionContext;


extern void AfterXactConnectionHandling(bool isCommit);
extern void InitializeConnectionManagement(void);

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
extern MultiConnection * StartNodeUserDatabaseConnection(uint32 flags,
														 const char *hostname,
														 int32 port,
														 const char *user,
														 const char *database);
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
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);
extern bool IsCitusInitiatedRemoteBackend(void);
extern void MarkConnectionConnected(MultiConnection *connection);

/* time utilities */
extern double MillisecondsPassedSince(instr_time moment);
extern long MillisecondsToTimeout(instr_time start, long msAfterStart);

#if PG_VERSION_NUM < 140000
extern void WarmUpConnParamsHash(void);
#endif
#endif /* CONNECTION_MANAGMENT_H */
