/*-------------------------------------------------------------------------
 *
 * subscription.c
 *    Commands for creating subscriptions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "safe_lib.h"

#include <string.h>

#include "commands/defrem.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/pg_version_constants.h"
#include "distributed/version_compat.h"
#include "libpq-fe.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"


static char * GenerateConninfoWithAuth(char *conninfo);

/*
 * ProcessCreateSubscriptionStmt looks for a special citus_use_authinfo option.
 * If it is set to true, then we'll expand the node's authinfo into the create
 * statement (see GenerateConninfoWithAuth).
 */
Node *
ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt)
{
	ListCell *currCell = NULL;
	#if PG_VERSION_NUM < PG_VERSION_13
	ListCell *prevCell = NULL;
	#endif
	bool useAuthinfo = false;

	foreach(currCell, createSubStmt->options)
	{
		DefElem *defElem = (DefElem *) lfirst(currCell);

		if (strcmp(defElem->defname, "citus_use_authinfo") == 0)
		{
			useAuthinfo = defGetBoolean(defElem);

			createSubStmt->options = list_delete_cell_compat(createSubStmt->options,
															 currCell,
															 prevCell);

			break;
		}
		#if PG_VERSION_NUM < PG_VERSION_13
		prevCell = currCell;
		#endif
	}

	if (useAuthinfo)
	{
		createSubStmt->conninfo = GenerateConninfoWithAuth(createSubStmt->conninfo);
	}

	return (Node *) createSubStmt;
}


/*
 * GenerateConninfoWithAuth extracts the host and port from the provided libpq
 * conninfo string, using them to find an appropriate authinfo for the target
 * host. If such an authinfo is found, it is added to the (repalloc'd) string,
 * which is then returned.
 */
static char *
GenerateConninfoWithAuth(char *conninfo)
{
	StringInfo connInfoWithAuth = makeStringInfo();
	char *host = NULL, *user = NULL;
	int32 port = -1;
	PQconninfoOption *option = NULL, *optionArray = NULL;

	optionArray = PQconninfoParse(conninfo, NULL);
	if (optionArray == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("not a valid libpq connection info string: %s",
							   conninfo)));
	}

	for (option = optionArray; option->keyword != NULL; option++)
	{
		if (option->val == NULL || option->val[0] == '\0')
		{
			continue;
		}

		if (strcmp(option->keyword, "host") == 0)
		{
			host = option->val;
		}
		else if (strcmp(option->keyword, "port") == 0)
		{
			port = pg_strtoint32(option->val);
		}
		else if (strcmp(option->keyword, "user") == 0)
		{
			user = option->val;
		}
	}

	/*
	 * In case of repetition of parameters in connection strings, last value
	 * wins. So first add the provided connection string, then global
	 * connection parameters, then node specific ones.
	 *
	 * Note that currently lists of parameters in pg_dist_authnode and
	 * citus.node_conninfo do not overlap.
	 *
	 * The only overlapping parameter between these three lists is
	 * connect_timeout, which is assigned in conninfo (generated
	 * by CreateShardMoveSubscription) and is also allowed in
	 * citus.node_conninfo. Prioritizing the value in citus.node_conninfo
	 * over conninfo gives user the power to control this value.
	 */
	appendStringInfo(connInfoWithAuth, "%s %s", conninfo, NodeConninfo);
	if (host != NULL && port > 0 && user != NULL)
	{
		char *nodeAuthInfo = GetAuthinfo(host, port, user);
		appendStringInfo(connInfoWithAuth, " %s", nodeAuthInfo);
	}

	PQconninfoFree(optionArray);

	return connInfoWithAuth->data;
}
