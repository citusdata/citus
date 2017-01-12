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
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

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

	/* mark returned connection as having session lifespan */
	SESSION_LIFESPAN = 1 << 1,

	FOR_DDL = 1 << 2,

	FOR_DML = 1 << 3
};


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

	/* is the connection intended to be kept after transaction end */
	bool sessionLifespan;

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

/* maximum duration to wait for connection */
extern int NodeConnectionTimeout;

/* the hash table */
extern HTAB *ConnectionHash;

/* context for all connection and transaction related memory */
extern struct MemoryContextData *ConnectionContext;


extern void AfterXactConnectionHandling(bool isCommit);
extern void InitializeConnectionManagement(void);


/* Low-level connection establishment APIs */
extern MultiConnection * GetNodeConnection(uint32 flags, const char *hostname,
										   int32 port);
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
extern MultiConnection * GetConnectionFromPGconn(struct pg_conn *pqConn);
extern void CloseNodeConnectionsAfterTransaction(char *nodeName, int nodePort);
extern void CloseConnection(MultiConnection *connection);
extern void CloseConnectionByPGconn(struct pg_conn *pqConn);

/* dealing with a connection */
extern void FinishConnectionListEstablishment(List *multiConnectionList);
extern void FinishConnectionEstablishment(MultiConnection *connection);
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);


#endif /* CONNECTION_MANAGMENT_H */
