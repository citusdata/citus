/*-------------------------------------------------------------------------
 *
 * connection_management.h
 *   Central management of connections and their life-cycle
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONNECTION_MANAGMENT_H
#define CONNECTION_MANAGMENT_H

#include "distributed/transaction_management.h"
#include "distributed/remote_transaction.h"
#include "lib/ilist.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

/* used for libpq commands that get an error buffer. Postgres docs recommend 256. */
#define ERROR_BUFFER_SIZE 256

/* default notice level */
#define DEFAULT_CITUS_NOTICE_LEVEL DEBUG1

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

	/* open a connection per (co-located set of) placement(s) */
	CONNECTION_PER_PLACEMENT = 1 << 3
};

typedef enum MultiConnectionState
{
	MULTI_CONNECTION_INITIAL,
	MULTI_CONNECTION_CONNECTING,
	MULTI_CONNECTION_CONNECTED,
	MULTI_CONNECTION_FAILED,
	MULTI_CONNECTION_LOST
} MultiConnectionState;

/* declaring this directly above makes uncrustify go crazy */
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

	/* time connection establishment was started, for timeout */
	TimestampTz connectionStart;

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

/* parameters used for outbound connections */
extern char *NodeConninfo;

/* the hash table */
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
extern bool CheckConninfo(const char *conninfo, const char **whitelist,
						  Size whitelistLength, char **errmsg);


/* Low-level connection establishment APIs */
extern MultiConnection * GetNodeConnection(uint32 flags, const char *hostname,
										   int32 port);
extern MultiConnection * GetNonDataAccessConnection(const char *hostname, int32 port);
extern MultiConnection * StartNonDataAccessConnection(const char *hostname, int32 port);
extern MultiConnection * StartNodeConnection(uint32 flags, const char *hostname,
											 int32 port);
extern MultiConnection * GetNodeUserDatabaseConnection(uint32 flags, const char *hostname,
													   int32 port, const char *user, const
													   char *database);
extern MultiConnection * StartNodeUserDatabaseConnection(uint32 flags,
														 const char *hostname,
														 int32 port,
														 const char *user,
														 const char *database);
extern void CloseNodeConnectionsAfterTransaction(char *nodeName, int nodePort);
extern void CloseConnection(MultiConnection *connection);
extern void ShutdownConnection(MultiConnection *connection);

/* dealing with a connection */
extern void FinishConnectionListEstablishment(List *multiConnectionList);
extern void FinishConnectionEstablishment(MultiConnection *connection);
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);
extern long DeadlineTimestampTzToTimeout(TimestampTz deadline);

/* dealing with notice handler */
extern void SetCitusNoticeProcessor(MultiConnection *connection);
extern void SetCitusNoticeLevel(int level);
extern char * TrimLogLevel(const char *message);
extern void UnsetCitusNoticeLevel(void);


#endif /* CONNECTION_MANAGMENT_H */
