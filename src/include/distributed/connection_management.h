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
#include "lib/ilist.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

#define CLIENT_CONNECT_TIMEOUT_SECONDS_INT 5

/* forward declare, to avoid forcing large headers on everyone */
struct pg_conn; /* target of the PGconn typedef */
struct MemoryContextData;

/*
 * Flags determining connection establishment behaviour.
 */
enum MultiConnectionMode
{
	/* connection should be part of the coordinated transaction */
	IN_TRANSACTION = 1 << 1,

	/* connection should be claimed exclusively for the caller */
	CLAIM_EXCLUSIVELY = 1 << 2,
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
	struct pg_conn *conn;

	/* is the connection currently in use, and shouldn't be used by anything else */
	bool claimedExclusively;

	/* is the connection currently part of the coordinated transaction */
	bool activeInTransaction;

	/* time connection establishment was started, for timeout */
	TimestampTz connectionStart;

	dlist_node node;
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

/* the hash table */
extern HTAB *ConnectionHash;

/* context for all connection and transaction related memory */
extern struct MemoryContextData *ConnectionContext;


extern void AfterXactResetConnections(bool isCommit);
extern void InitializeConnectionManagement(void);


/* Low-level connection establishment APIs */
extern MultiConnection * GetNodeConnection(const char *hostname, int32 port,
										   uint32 flags);
extern MultiConnection * StartNodeConnection(const char *hostname, int32 port,
											 uint32 flags);
extern MultiConnection * GetNodeUserDatabaseConnection(const char *hostname, int32 port,
													   const char *user,
													   const char *database,
													   uint32 flags);
extern MultiConnection * StartNodeUserDatabaseConnection(const char *hostname,
														 int32 port,
														 const char *user,
														 const char *database,
														 uint32 flags);
extern void CloseConnectionByPGconn(struct pg_conn *pqConn);

/* dealing with a connection */
extern void FinishConnectionEstablishment(MultiConnection *connection);
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);


#endif /* CONNECTION_MANAGMENT_H */
