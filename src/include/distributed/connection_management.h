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
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

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
	/* allow establishment of new connections */
	NEW_CONNECTION = 1 << 0,

	/* allow use of pre-established connections */
	CACHED_CONNECTION = 1 << 1,

	/* mark returned connection having session lifespan */
	SESSION_LIFESPAN = 1 << 2,

	/* the connection will be used for DML */
	FOR_DML = 1 << 3,

	/* the connection will be used for DDL */
	FOR_DDL = 1 << 4,

	/* failures on this connection will fail entire coordinated transaction */
	CRITICAL_CONNECTION = 1 << 5
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

	/* is the connection intended to be kept after transaction end */
	bool sessionLifespan;

	/* is the connection currently in use, and shouldn't be used by anything else */
	bool claimedExclusively;

	/* has the connection been used in the current coordinated transaction? */
	bool activeInTransaction;
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
	List *connections;
} ConnectionHashEntry;

/* the hash table */
extern HTAB *ConnectionHash;

/* context for all connection and transaction related memory */
extern struct MemoryContextData *ConnectionContext;


extern void AtEOXact_Connections(bool isCommit);
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

/* dealing with a connection */
extern void FinishConnectionEstablishment(MultiConnection *connection);
extern void ClaimConnectionExclusively(MultiConnection *connection);
extern void UnclaimConnection(MultiConnection *connection);


#endif /* CONNECTION_MANAGMENT_H */
