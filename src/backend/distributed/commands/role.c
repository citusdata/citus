#include "postgres.h"

#include "nodes/parsenodes.h"
#include "utils/syscache.h"
#include "access/table.h"
#include "catalog/pg_authid.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "access/heapam.h"
#include "distributed/worker_transaction.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/master_protocol.h"

List * ProcessAlterRoleStmt(AlterRoleStmt *stmt, const char *queryString);
static const char * CreateSetPasswordQueryForWorkerNodes(const char *rolename);
List * CreateSetPasswordQueriesOfUsersWithPassword(void);


bool EnablePasswordPropagation = true;

/*
 * ProcessAlterRoleStmt actually creates the plan we need to execute for alter
 * role statement.
 */
List *
ProcessAlterRoleStmt(AlterRoleStmt *stmt, const char *queryString)
{
	ListCell *optionCell = NULL;
	List *commands = NIL;
	const char *setPasswordQuery = NULL;

	if (!IsCoordinator())
	{
		return NIL;
	}

	foreach(optionCell, stmt->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		if (EnablePasswordPropagation && strcasecmp(option->defname, "password") == 0)
		{
			setPasswordQuery = CreateSetPasswordQueryForWorkerNodes(
				stmt->role->rolename);
			commands = lappend(commands, (void *) setPasswordQuery);
		}
	}
	if (list_length(commands) <= 0)
	{
		return NIL;
	}
	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * CreateSetPasswordQueryForWorkerNodes creates the query for updating the
 * password of the roles in the worker nodes.
 *
 * Uses the set_password_if_role_exists UDF.
 *
 * The passwords are found in the pg_authid table.
 */
static const char *
CreateSetPasswordQueryForWorkerNodes(const char *rolename)
{
	StringInfoData setPasswordQueryBuffer;
	Relation pgAuthId;
	HeapTuple tuple;
	bool isNull;
	TupleDesc pgAuthIdDescription;
	Datum passwordDatum;

	pgAuthId = table_open(AuthIdRelationId, AccessShareLock);
	tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(rolename));

	pgAuthIdDescription = RelationGetDescr(pgAuthId);
	passwordDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword,
								 pgAuthIdDescription, &isNull);

	table_close(pgAuthId, AccessShareLock);
	ReleaseSysCache(tuple);

	initStringInfo(&setPasswordQueryBuffer);
	appendStringInfo(&setPasswordQueryBuffer,
					 "SELECT set_password_if_role_exists('%s', '%s')",
					 rolename,
					 TextDatumGetCString(passwordDatum));

	return pstrdup(setPasswordQueryBuffer.data);
}


/*
 * CreateSetPasswordQueriesOfUsersWithPassword creates the set password queries
 * for users whose password exists in the pg_authid table.
 */
List *
CreateSetPasswordQueriesOfUsersWithPassword()
{
	Relation pgAuthId;
	TableScanDesc scan;
	HeapTuple tuple;
	bool isNull;
	TupleDesc pgAuthIdDescription;
	List *commands = NIL;
	const char *setPasswordQuery = NULL;

	pgAuthId = table_open(AuthIdRelationId, AccessShareLock);
	scan = table_beginscan_catalog(pgAuthId, 0, NULL);
	pgAuthIdDescription = RelationGetDescr(pgAuthId);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		heap_getattr(tuple, Anum_pg_authid_rolpassword, pgAuthIdDescription, &isNull);
		if (!isNull)
		{
			setPasswordQuery = CreateSetPasswordQueryForWorkerNodes(
				NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
			commands = lappend(commands, (void *) setPasswordQuery);
		}
	}

	table_endscan(scan);
	table_close(pgAuthId, AccessShareLock);

	return commands;
}
