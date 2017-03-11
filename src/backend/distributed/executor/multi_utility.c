/*-------------------------------------------------------------------------
 * multi_utility.c
 *	  Citus utility hook and related functionality.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <string.h>

#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/prepare.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/multi_utility.h" /* IWYU pragma: keep */
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/transmit.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "parser/analyze.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"


bool EnableDDLPropagation = true; /* ddl propagation is enabled */


/*
 * This struct defines the state for the callback for drop statements.
 * It is copied as it is from commands/tablecmds.c in Postgres source.
 */
struct DropRelationCallbackState
{
	char relkind;
	Oid heapOid;
	bool concurrent;
};


/* Local functions forward declarations for Transmit statement */
static bool IsTransmitStmt(Node *parsetree);
static void VerifyTransmitStmt(CopyStmt *copyStatement);

/* Local functions forward declarations for processing distributed table commands */
static Node * ProcessCopyStmt(CopyStmt *copyStatement, char *completionTag,
							  bool *commandMustRunAsOwner);
static DDLJob * PlanIndexStmt(IndexStmt *createIndexStatement,
							  const char *createIndexCommand);
static DDLJob * PlanDropIndexStmt(DropStmt *dropIndexStatement,
								  const char *dropIndexCommand);
static DDLJob * PlanAlterTableStmt(AlterTableStmt *alterTableStatement,
								   const char *alterTableCommand);
static Node * WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
										  const char *alterTableCommand);
static DDLJob * PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
										  const char *alterObjectSchemaCommand);
static void ProcessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand);
static bool IsSupportedDistributedVacuumStmt(Oid relationId, VacuumStmt *vacuumStmt);
static List * VacuumTaskList(Oid relationId, VacuumStmt *vacuumStmt);
static StringInfo DeparseVacuumStmtPrefix(VacuumStmt *vacuumStmt);
static char * DeparseVacuumColumnNames(List *columnNameList);


/* Local functions forward declarations for unsupported command checks */
static void ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement);
static void ErrorIfUnsupportedDropIndexStmt(DropStmt *dropIndexStatement);
static void ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement);
static void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
static void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);
static void ErrorIfUnsupportedTruncateStmt(TruncateStmt *truncateStatement);
static bool OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId);
static void ErrorIfDistributedRenameStmt(RenameStmt *renameStatement);

/* Local functions forward declarations for helper functions */
static void CreateLocalTable(RangeVar *relation, char *nodeName, int32 nodePort);
static bool IsAlterTableRenameStmt(RenameStmt *renameStatement);
static void ExecuteDistributedDDLJob(DDLJob *ddlJob);
static void ShowNoticeIfNotUsing2PC(void);
static List * DDLTaskList(Oid relationId, const char *commandString);
static List * ForeignKeyTaskList(Oid leftRelationId, Oid rightRelationId,
								 const char *commandString);
static void RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid,
										 void *arg);
static void CheckCopyPermissions(CopyStmt *copyStatement);
static List * CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);


static bool warnedUserAbout2PC = false;


/*
 * Utility for handling citus specific concerns around utility statements.
 *
 * There's two basic types of concerns here:
 * 1) Intercept utility statements that run after distributed query
 *    execution. At this stage, the Create Table command for the master node's
 *    temporary table has been executed, and this table's relationId is
 *    visible to us. We can therefore update the relationId in master node's
 *    select query.
 * 2) Handle utility statements on distributed tables that the core code can't
 *    handle.
 */
void
multi_ProcessUtility(Node *parsetree,
					 const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 DestReceiver *dest,
					 char *completionTag)
{
	bool commandMustRunAsOwner = false;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	DDLJob *ddlJob = NULL;

	if (IsA(parsetree, TransactionStmt))
	{
		/*
		 * Transaction statements (e.g. ABORT, COMMIT) can be run in aborted
		 * transactions in which case a lot of checks cannot be done safely in
		 * that state. Since we never need to intercept transaction statements,
		 * skip our checks and immediately fall into standard_ProcessUtility.
		 */
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);

		return;
	}

	if (!CitusHasBeenLoaded())
	{
		/*
		 * Ensure that utility commands do not behave any differently until CREATE
		 * EXTENSION is invoked.
		 */
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);

		return;
	}

	/*
	 * TRANSMIT used to be separate command, but to avoid patching the grammar
	 * it's no overlaid onto COPY, but with FORMAT = 'transmit' instead of the
	 * normal FORMAT options.
	 */
	if (IsTransmitStmt(parsetree))
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;

		VerifyTransmitStmt(copyStatement);

		/* ->relation->relname is the target file in our overloaded COPY */
		if (copyStatement->is_from)
		{
			RedirectCopyDataToRegularFile(copyStatement->relation->relname);
		}
		else
		{
			SendRegularFile(copyStatement->relation->relname);
		}

		/* Don't execute the faux copy statement */
		return;
	}

	if (IsA(parsetree, CopyStmt))
	{
		parsetree = ProcessCopyStmt((CopyStmt *) parsetree, completionTag,
									&commandMustRunAsOwner);

		if (parsetree == NULL)
		{
			return;
		}
	}

	if (IsA(parsetree, CreateSeqStmt))
	{
		ErrorIfUnsupportedSeqStmt((CreateSeqStmt *) parsetree);
	}

	if (IsA(parsetree, AlterSeqStmt))
	{
		ErrorIfDistributedAlterSeqOwnedBy((AlterSeqStmt *) parsetree);
	}

	if (IsA(parsetree, TruncateStmt))
	{
		ErrorIfUnsupportedTruncateStmt((TruncateStmt *) parsetree);
	}

	/*
	 * DDL commands are propagated to workers only if EnableDDLPropagation is
	 * set to true and the current node is the coordinator
	 */
	if (EnableDDLPropagation)
	{
		if (IsA(parsetree, IndexStmt))
		{
			ddlJob = PlanIndexStmt((IndexStmt *) parsetree, queryString);
		}

		if (IsA(parsetree, DropStmt))
		{
			DropStmt *dropStatement = (DropStmt *) parsetree;
			if (dropStatement->removeType == OBJECT_INDEX)
			{
				ddlJob = PlanDropIndexStmt(dropStatement, queryString);
			}
		}

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE)
			{
				ddlJob = PlanAlterTableStmt(alterTableStmt, queryString);
			}
		}

		/*
		 * ALTER TABLE ... RENAME statements have their node type as RenameStmt and
		 * not AlterTableStmt. So, we intercept RenameStmt to tackle these commands.
		 */
		if (IsA(parsetree, RenameStmt))
		{
			RenameStmt *renameStmt = (RenameStmt *) parsetree;
			if (IsAlterTableRenameStmt(renameStmt))
			{
				ErrorIfDistributedRenameStmt(renameStmt);
			}
		}

		/*
		 * ALTER ... SET SCHEMA statements have their node type as AlterObjectSchemaStmt.
		 * So, we intercept AlterObjectSchemaStmt to tackle these commands.
		 */
		if (IsA(parsetree, AlterObjectSchemaStmt))
		{
			AlterObjectSchemaStmt *setSchemaStmt = (AlterObjectSchemaStmt *) parsetree;
			ddlJob = PlanAlterObjectSchemaStmt(setSchemaStmt, queryString);
		}

		/*
		 * ALTER TABLE ALL IN TABLESPACE statements have their node type as
		 * AlterTableMoveAllStmt. At the moment we do not support this functionality in
		 * the distributed environment. We warn out here.
		 */
		if (IsA(parsetree, AlterTableMoveAllStmt))
		{
			ereport(WARNING, (errmsg("not propagating ALTER TABLE ALL IN TABLESPACE "
									 "commands to worker nodes"),
							  errhint("Connect to worker nodes directly to manually "
									  "move all tables.")));
		}
	}
	else
	{
		/*
		 * citus.enable_ddl_propagation is disabled, which means that PostgreSQL
		 * should handle the DDL command on a distributed table directly, without
		 * Citus intervening. Advanced Citus users use this to implement their own
		 * DDL propagation. We also use it to avoid re-propagating DDL commands
		 * when changing MX tables on workers. Below, we also make sure that DDL
		 * commands don't run queries that might get intercepted by Citus and error
		 * out, specifically we skip validation in foreign keys.
		 */

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE)
			{
				/*
				 * When issuing an ALTER TABLE ... ADD FOREIGN KEY command, the
				 * the validation step should be skipped on the distributed table.
				 * Therefore, we check whether the given ALTER TABLE statement is a
				 * FOREIGN KEY constraint and if so disable the validation step.
				 * Note that validation is done on the shard level when DDL
				 * propagation is enabled.
				 */
				parsetree = WorkerProcessAlterTableStmt(alterTableStmt, queryString);
			}
		}
	}

	/* inform the user about potential caveats */
	if (IsA(parsetree, CreatedbStmt))
	{
		ereport(NOTICE, (errmsg("Citus partially supports CREATE DATABASE for "
								"distributed databases"),
						 errdetail("Citus does not propagate CREATE DATABASE "
								   "command to workers"),
						 errhint("You can manually create a database and its "
								 "extensions on workers.")));
	}
	else if (IsA(parsetree, CreateRoleStmt))
	{
		ereport(NOTICE, (errmsg("not propagating CREATE ROLE/USER commands to worker"
								" nodes"),
						 errhint("Connect to worker nodes directly to manually create all"
								 " necessary users and roles.")));
	}

	if (commandMustRunAsOwner)
	{
		GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
		SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);
	}

	/* now drop into standard process utility */
	standard_ProcessUtility(parsetree, queryString, context,
							params, dest, completionTag);

	if (commandMustRunAsOwner)
	{
		SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	}

	if (ddlJob != NULL)
	{
		ExecuteDistributedDDLJob(ddlJob);
	}

	/* we run VacuumStmt after standard hook to benefit from its checks and locking */
	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacuumStmt = (VacuumStmt *) parsetree;

		ProcessVacuumStmt(vacuumStmt, queryString);
	}
}


/* Is the passed in statement a transmit statement? */
static bool
IsTransmitStmt(Node *parsetree)
{
	if (IsA(parsetree, CopyStmt))
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;
		ListCell *optionCell = NULL;

		/* Extract options from the statement node tree */
		foreach(optionCell, copyStatement->options)
		{
			DefElem *defel = (DefElem *) lfirst(optionCell);

			if (strncmp(defel->defname, "format", NAMEDATALEN) == 0 &&
				strncmp(defGetString(defel), "transmit", NAMEDATALEN) == 0)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * VerifyTransmitStmt checks that the passed in command is a valid transmit
 * statement. Raise ERROR if not.
 *
 * Note that only 'toplevel' options in the CopyStmt struct are checked, and
 * that verification of the target files existance is not done here.
 */
static void
VerifyTransmitStmt(CopyStmt *copyStatement)
{
	/* do some minimal option verification */
	if (copyStatement->relation == NULL ||
		copyStatement->relation->relname == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("FORMAT 'transmit' requires a target file")));
	}

	if (copyStatement->filename != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("FORMAT 'transmit' only accepts STDIN/STDOUT"
							   " as input/output")));
	}

	if (copyStatement->query != NULL ||
		copyStatement->attlist != NULL ||
		copyStatement->is_program)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("FORMAT 'transmit' does not accept query, attribute list"
							   " or PROGRAM parameters ")));
	}
}


/*
 * ProcessCopyStmt handles Citus specific concerns for COPY like supporting
 * COPYing from distributed tables and preventing unsupported actions. The
 * function returns a modified COPY statement to be executed, or NULL if no
 * further processing is needed.
 *
 * commandMustRunAsOwner is an output parameter used to communicate to the caller whether
 * the copy statement should be executed using elevated privileges. If
 * ProcessCopyStmt that is required, a call to CheckCopyPermissions will take
 * care of verifying the current user's permissions before ProcessCopyStmt
 * returns.
 */
static Node *
ProcessCopyStmt(CopyStmt *copyStatement, char *completionTag, bool *commandMustRunAsOwner)
{
	*commandMustRunAsOwner = false; /* make sure variable is initialized */

	/*
	 * We check whether a distributed relation is affected. For that, we need to open the
	 * relation. To prevent race conditions with later lookups, lock the table, and modify
	 * the rangevar to include the schema.
	 */
	if (copyStatement->relation != NULL)
	{
		bool isDistributedRelation = false;
		bool isCopyFromWorker = IsCopyFromWorker(copyStatement);

		if (isCopyFromWorker)
		{
			RangeVar *relation = copyStatement->relation;
			NodeAddress *masterNodeAddress = MasterNodeAddress(copyStatement);
			char *nodeName = masterNodeAddress->nodeName;
			int32 nodePort = masterNodeAddress->nodePort;

			CreateLocalTable(relation, nodeName, nodePort);

			/*
			 * We expect copy from worker to be on a distributed table; otherwise,
			 * it fails in CitusCopyFrom() while checking the partition method.
			 */
			isDistributedRelation = true;
		}
		else
		{
			bool isFrom = copyStatement->is_from;
			Relation copiedRelation = NULL;

			/* consider using RangeVarGetRelidExtended to check perms before locking */
			copiedRelation = heap_openrv(copyStatement->relation,
										 isFrom ? RowExclusiveLock : AccessShareLock);

			isDistributedRelation = IsDistributedTable(RelationGetRelid(copiedRelation));

			/* ensure future lookups hit the same relation */
			copyStatement->relation->schemaname = get_namespace_name(
				RelationGetNamespace(copiedRelation));

			heap_close(copiedRelation, NoLock);
		}

		if (isDistributedRelation)
		{
			if (copyStatement->is_from)
			{
				/* check permissions, we're bypassing postgres' normal checks */
				if (!isCopyFromWorker)
				{
					CheckCopyPermissions(copyStatement);
				}

				CitusCopyFrom(copyStatement, completionTag);
				return NULL;
			}
			else if (!copyStatement->is_from)
			{
				/*
				 * The copy code only handles SELECTs in COPY ... TO on master tables,
				 * as that can be done non-invasively. To handle COPY master_rel TO
				 * the copy statement is replaced by a generated select statement.
				 */
				ColumnRef *allColumns = makeNode(ColumnRef);
				SelectStmt *selectStmt = makeNode(SelectStmt);
				ResTarget *selectTarget = makeNode(ResTarget);

				allColumns->fields = list_make1(makeNode(A_Star));
				allColumns->location = -1;

				selectTarget->name = NULL;
				selectTarget->indirection = NIL;
				selectTarget->val = (Node *) allColumns;
				selectTarget->location = -1;

				selectStmt->targetList = list_make1(selectTarget);
				selectStmt->fromClause = list_make1(copyObject(copyStatement->relation));

				/* replace original statement */
				copyStatement = copyObject(copyStatement);
				copyStatement->relation = NULL;
				copyStatement->query = (Node *) selectStmt;
			}
		}
	}


	if (copyStatement->filename != NULL && !copyStatement->is_program)
	{
		const char *filename = copyStatement->filename;

		if (CacheDirectoryElement(filename))
		{
			/*
			 * Only superusers are allowed to copy from a file, so we have to
			 * become superuser to execute copies to/from files used by citus'
			 * query execution.
			 *
			 * XXX: This is a decidedly suboptimal solution, as that means
			 * that triggers, input functions, etc. run with elevated
			 * privileges.  But this is better than not being able to run
			 * queries as normal user.
			 */
			*commandMustRunAsOwner = true;

			/*
			 * Have to manually check permissions here as the COPY is will be
			 * run as a superuser.
			 */
			if (copyStatement->relation != NULL)
			{
				CheckCopyPermissions(copyStatement);
			}

			/*
			 * Check if we have a "COPY (query) TO filename". If we do, copy
			 * doesn't accept relative file paths. However, SQL tasks that get
			 * assigned to worker nodes have relative paths. We therefore
			 * convert relative paths to absolute ones here.
			 */
			if (copyStatement->relation == NULL &&
				!copyStatement->is_from &&
				!is_absolute_path(filename))
			{
				copyStatement->filename = make_absolute_path(filename);
			}
		}
	}


	return (Node *) copyStatement;
}


/*
 * PlanIndexStmt processes create index statements for distributed tables.
 * The function first checks if the statement belongs to a distributed table
 * or not. If it does, then it executes distributed logic for the command.
 *
 * The function returns the IndexStmt node for the command to be executed on the
 * master node table.
 */
static DDLJob *
PlanIndexStmt(IndexStmt *createIndexStatement, const char *createIndexCommand)
{
	DDLJob *ddlJob = NULL;

	/*
	 * We first check whether a distributed relation is affected. For that, we need to
	 * open the relation. To prevent race conditions with later lookups, lock the table,
	 * and modify the rangevar to include the schema.
	 */
	if (createIndexStatement->relation != NULL)
	{
		Relation relation = NULL;
		Oid relationId = InvalidOid;
		bool isDistributedRelation = false;
		char *namespaceName = NULL;
		LOCKMODE lockmode = ShareLock;

		/*
		 * We don't support concurrently creating indexes for distributed
		 * tables, but till this point, we don't know if it is a regular or a
		 * distributed table.
		 */
		if (createIndexStatement->concurrent)
		{
			lockmode = ShareUpdateExclusiveLock;
		}

		/*
		 * XXX: Consider using RangeVarGetRelidExtended with a permission
		 * checking callback. Right now we'll acquire the lock before having
		 * checked permissions, and will only fail when executing the actual
		 * index statements.
		 */
		relation = heap_openrv(createIndexStatement->relation, lockmode);
		relationId = RelationGetRelid(relation);

		isDistributedRelation = IsDistributedTable(relationId);

		/* ensure future lookups hit the same relation */
		namespaceName = get_namespace_name(RelationGetNamespace(relation));
		createIndexStatement->relation->schemaname = namespaceName;

		heap_close(relation, NoLock);

		if (isDistributedRelation)
		{
			Oid namespaceId = InvalidOid;
			Oid indexRelationId = InvalidOid;
			char *indexName = createIndexStatement->idxname;

			ErrorIfUnsupportedIndexStmt(createIndexStatement);

			namespaceId = get_namespace_oid(namespaceName, false);
			indexRelationId = get_relname_relid(indexName, namespaceId);

			/* if index does not exist, send the command to workers */
			if (!OidIsValid(indexRelationId))
			{
				ddlJob = palloc0(sizeof(DDLJob));
				ddlJob->targetRelationId = relationId;
				ddlJob->commandString = createIndexCommand;
				ddlJob->taskList = DDLTaskList(relationId, createIndexCommand);
			}
			else if (!createIndexStatement->if_not_exists)
			{
				/* if the index exists and there is no IF NOT EXISTS clause, error */
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
								errmsg("relation \"%s\" already exists", indexName)));
			}
		}
	}

	return ddlJob;
}


/*
 * PlanDropIndexStmt processes drop index statements for distributed tables.
 * The function first checks if the statement belongs to a distributed table
 * or not. If it does, then it executes distributed logic for the command.
 *
 * The function returns the DropStmt node for the command to be executed on the
 * master node table.
 */
static DDLJob *
PlanDropIndexStmt(DropStmt *dropIndexStatement, const char *dropIndexCommand)
{
	DDLJob *ddlJob = NULL;
	ListCell *dropObjectCell = NULL;
	Oid distributedIndexId = InvalidOid;
	Oid distributedRelationId = InvalidOid;

	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	/* check if any of the indexes being dropped belong to a distributed table */
	foreach(dropObjectCell, dropIndexStatement->objects)
	{
		Oid indexId = InvalidOid;
		Oid relationId = InvalidOid;
		bool isDistributedRelation = false;
		struct DropRelationCallbackState state;
		bool missingOK = true;
		bool noWait = false;
		LOCKMODE lockmode = AccessExclusiveLock;

		List *objectNameList = (List *) lfirst(dropObjectCell);
		RangeVar *rangeVar = makeRangeVarFromNameList(objectNameList);

		/*
		 * We don't support concurrently dropping indexes for distributed
		 * tables, but till this point, we don't know if it is a regular or a
		 * distributed table.
		 */
		if (dropIndexStatement->concurrent)
		{
			lockmode = ShareUpdateExclusiveLock;
		}

		/*
		 * The next few statements are based on RemoveRelations() in
		 * commands/tablecmds.c in Postgres source.
		 */
		AcceptInvalidationMessages();

		state.relkind = RELKIND_INDEX;
		state.heapOid = InvalidOid;
		state.concurrent = dropIndexStatement->concurrent;
		indexId = RangeVarGetRelidExtended(rangeVar, lockmode, missingOK,
										   noWait, RangeVarCallbackForDropIndex,
										   (void *) &state);

		/*
		 * If the index does not exist, we don't do anything here, and allow
		 * postgres to throw appropriate error or notice message later.
		 */
		if (!OidIsValid(indexId))
		{
			continue;
		}

		relationId = IndexGetRelation(indexId, false);
		isDistributedRelation = IsDistributedTable(relationId);
		if (isDistributedRelation)
		{
			distributedIndexId = indexId;
			distributedRelationId = relationId;
			break;
		}
	}

	if (OidIsValid(distributedIndexId))
	{
		ddlJob = palloc0(sizeof(DDLJob));

		ErrorIfUnsupportedDropIndexStmt(dropIndexStatement);

		/* if it is supported, go ahead and execute the command */
		ddlJob->targetRelationId = distributedRelationId;
		ddlJob->commandString = dropIndexCommand;
		ddlJob->taskList = DDLTaskList(distributedRelationId, dropIndexCommand);
	}

	return ddlJob;
}


/*
 * PlanAlterTableStmt processes alter table statements for distributed tables.
 * The function first checks if the statement belongs to a distributed table
 * or not. If it does, then it executes distributed logic for the command.
 *
 * The function returns the AlterTableStmt node for the command to be executed on the
 * master node table.
 */
static DDLJob *
PlanAlterTableStmt(AlterTableStmt *alterTableStatement, const char *alterTableCommand)
{
	DDLJob *ddlJob = NULL;
	LOCKMODE lockmode = 0;
	Oid leftRelationId = InvalidOid;
	Oid rightRelationId = InvalidOid;
	bool isDistributedRelation = false;
	List *commandList = NIL;
	ListCell *commandCell = NULL;

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return NULL;
	}

	lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return NULL;
	}

	isDistributedRelation = IsDistributedTable(leftRelationId);
	if (!isDistributedRelation)
	{
		return NULL;
	}

	ErrorIfUnsupportedAlterTableStmt(alterTableStatement);

	/*
	 * We check if there is a ADD FOREIGN CONSTRAINT command in sub commands list.
	 * If there is we assign referenced releation id to rightRelationId and we also
	 * set skip_validation to true to prevent PostgreSQL to verify validity of the
	 * foreign constraint in master. Validity will be checked in workers anyway.
	 */
	commandList = alterTableStatement->cmds;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				/*
				 * We only support ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY, if it is
				 * only subcommand of ALTER TABLE. It was already checked in
				 * ErrorIfUnsupportedAlterTableStmt.
				 */
				Assert(commandList->length <= 1);

				rightRelationId = RangeVarGetRelid(constraint->pktable, lockmode,
												   alterTableStatement->missing_ok);

				/*
				 * Foreign constraint validations will be done in workers. If we do not
				 * set this flag, PostgreSQL tries to do additional checking when we drop
				 * to standard_ProcessUtility. standard_ProcessUtility tries to open new
				 * connections to workers to verify foreign constraints while original
				 * transaction is in process, which causes deadlock.
				 */
				constraint->skip_validation = true;
			}
		}
	}

	ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = leftRelationId;
	ddlJob->commandString = alterTableCommand;

	if (rightRelationId)
	{
		ddlJob->taskList = ForeignKeyTaskList(leftRelationId, rightRelationId,
											  alterTableCommand);
	}
	else
	{
		ddlJob->taskList = DDLTaskList(leftRelationId, alterTableCommand);
	}

	return ddlJob;
}


/*
 * WorkerProcessAlterTableStmt checks and processes the alter table statement to be
 * worked on the distributed table of the worker node. Currently, it only processes
 * ALTER TABLE ... ADD FOREIGN KEY command to skip the validation step.
 */
static Node *
WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
							const char *alterTableCommand)
{
	LOCKMODE lockmode = 0;
	Oid leftRelationId = InvalidOid;
	bool isDistributedRelation = false;
	List *commandList = NIL;
	ListCell *commandCell = NULL;

	/* first check whether a distributed relation is affected */
	if (alterTableStatement->relation == NULL)
	{
		return (Node *) alterTableStatement;
	}

	lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	leftRelationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	if (!OidIsValid(leftRelationId))
	{
		return (Node *) alterTableStatement;
	}

	isDistributedRelation = IsDistributedTable(leftRelationId);
	if (!isDistributedRelation)
	{
		return (Node *) alterTableStatement;
	}

	/*
	 * We check if there is a ADD FOREIGN CONSTRAINT command in sub commands list.
	 * If there is we assign referenced releation id to rightRelationId and we also
	 * set skip_validation to true to prevent PostgreSQL to verify validity of the
	 * foreign constraint in master. Validity will be checked in workers anyway.
	 */
	commandList = alterTableStatement->cmds;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = command->subtype;

		if (alterTableType == AT_AddConstraint)
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_FOREIGN)
			{
				/* foreign constraint validations will be done in shards. */
				constraint->skip_validation = true;
			}
		}
	}

	return (Node *) alterTableStatement;
}


/*
 * PlanAlterObjectSchemaStmt processes ALTER ... SET SCHEMA statements for distributed
 * objects. The function first checks if the statement belongs to a distributed objects
 * or not. If it does, then it checks whether given object is a table. If it is, we warn
 * out, since we do not support ALTER ... SET SCHEMA
 */
static DDLJob *
PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
						  const char *alterObjectSchemaCommand)
{
	Oid relationId = InvalidOid;
	bool noWait = false;

	if (alterObjectSchemaStmt->relation == NULL)
	{
		return NULL;
	}

	relationId = RangeVarGetRelidExtended(alterObjectSchemaStmt->relation,
										  AccessExclusiveLock,
										  alterObjectSchemaStmt->missing_ok,
										  noWait, NULL, NULL);

	/* first check whether a distributed relation is affected */
	if (!OidIsValid(relationId) || !IsDistributedTable(relationId))
	{
		return NULL;
	}

	/* warn out if a distributed relation is affected */
	ereport(WARNING, (errmsg("not propagating ALTER ... SET SCHEMA commands to "
							 "worker nodes"),
					  errhint("Connect to worker nodes directly to manually "
							  "change schemas of affected objects.")));

	return NULL;
}


/*
 * ProcessVacuumStmt processes vacuum statements that may need propagation to
 * distributed tables. If a VACUUM or ANALYZE command references a distributed
 * table, it is propagated to all involved nodes; otherwise, this function will
 * immediately exit after some error checking.
 *
 * Unlike most other Process functions within this file, this function does not
 * return a modified parse node, as it is expected that the local VACUUM or
 * ANALYZE has already been processed.
 */
static void
ProcessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand)
{
	Oid relationId = InvalidOid;
	List *taskList = NIL;
	bool supportedVacuumStmt = false;

	if (vacuumStmt->relation != NULL)
	{
		LOCKMODE lockMode = (vacuumStmt->options & VACOPT_FULL) ?
							AccessExclusiveLock : ShareUpdateExclusiveLock;

		relationId = RangeVarGetRelid(vacuumStmt->relation, lockMode, false);

		if (relationId == InvalidOid)
		{
			return;
		}
	}

	supportedVacuumStmt = IsSupportedDistributedVacuumStmt(relationId, vacuumStmt);
	if (!supportedVacuumStmt)
	{
		return;
	}

	taskList = VacuumTaskList(relationId, vacuumStmt);

	/* save old commit protocol to restore at xact end */
	Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
	SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
	MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

	ExecuteModifyTasksWithoutResults(taskList);
}


/*
 * IsSupportedDistributedVacuumStmt returns whether distributed execution of a
 * given VacuumStmt is supported. The provided relationId (if valid) represents
 * the table targeted by the provided statement.
 *
 * Returns true if the statement requires distributed execution and returns
 * false otherwise; however, this function will raise errors if the provided
 * statement needs distributed execution but contains unsupported options.
 */
static bool
IsSupportedDistributedVacuumStmt(Oid relationId, VacuumStmt *vacuumStmt)
{
	const char *stmtName = (vacuumStmt->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";

	if (vacuumStmt->relation == NULL)
	{
		/* WARN and exit early for unqualified VACUUM commands */
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", stmtName),
						  errhint("Provide a specific table in order to %s "
								  "distributed tables.", stmtName)));

		return false;
	}

	if (!OidIsValid(relationId) || !IsDistributedTable(relationId))
	{
		return false;
	}

	if (!EnableDDLPropagation)
	{
		/* WARN and exit early if DDL propagation is not enabled */
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", stmtName),
						  errhint("Set citus.enable_ddl_propagation to true in order to "
								  "send targeted %s commands to worker nodes.",
								  stmtName)));
	}

	if (vacuumStmt->options & VACOPT_VERBOSE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("the VERBOSE option is currently unsupported in "
							   "distributed %s commands", stmtName)));
	}

	return true;
}


/*
 * VacuumTaskList returns a list of tasks to be executed as part of processing
 * a VacuumStmt which targets a distributed relation.
 */
static List *
VacuumTaskList(Oid relationId, VacuumStmt *vacuumStmt)
{
	List *taskList = NIL;
	List *shardIntervalList = NIL;
	ListCell *shardIntervalCell = NULL;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;
	StringInfo vacuumString = DeparseVacuumStmtPrefix(vacuumStmt);
	const char *columnNames = DeparseVacuumColumnNames(vacuumStmt->va_cols);
	const int vacuumPrefixLen = vacuumString->len;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(relationId);

	/* lock relation metadata before getting shard list */
	LockRelationDistributionMetadata(relationId, ShareLock);

	shardIntervalList = LoadShardIntervalList(relationId);

	/* grab shard lock before getting placement list */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		Task *task = NULL;

		char *shardName = pstrdup(tableName);
		AppendShardIdToName(&shardName, shardInterval->shardId);
		shardName = quote_qualified_identifier(schemaName, shardName);

		vacuumString->len = vacuumPrefixLen;
		appendStringInfoString(vacuumString, shardName);
		appendStringInfoString(vacuumString, columnNames);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = pstrdup(vacuumString->data);
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * DeparseVacuumStmtPrefix returns a StringInfo appropriate for use as a prefix
 * during distributed execution of a VACUUM or ANALYZE statement. Callers may
 * reuse this prefix within a loop to generate shard-specific VACUUM or ANALYZE
 * statements.
 */
static StringInfo
DeparseVacuumStmtPrefix(VacuumStmt *vacuumStmt)
{
	StringInfo vacuumPrefix = makeStringInfo();
	int vacuumFlags = vacuumStmt->options;
	const int unsupportedFlags PG_USED_FOR_ASSERTS_ONLY = ~(
		VACOPT_ANALYZE |
#if (PG_VERSION_NUM >= 90600)
		VACOPT_DISABLE_PAGE_SKIPPING |
#endif
		VACOPT_FREEZE |
		VACOPT_FULL
		);

	/* determine actual command and block out its bit */
	if (vacuumFlags & VACOPT_VACUUM)
	{
		appendStringInfoString(vacuumPrefix, "VACUUM ");
		vacuumFlags &= ~VACOPT_VACUUM;
	}
	else
	{
		appendStringInfoString(vacuumPrefix, "ANALYZE ");
		vacuumFlags &= ~VACOPT_ANALYZE;
	}

	/* unsupported flags should have already been rejected */
	Assert((vacuumFlags & unsupportedFlags) == 0);

	/* if no flags remain, exit early */
	if (vacuumFlags == 0)
	{
		return vacuumPrefix;
	}

	/* otherwise, handle options */
	appendStringInfoChar(vacuumPrefix, '(');

	if (vacuumFlags & VACOPT_ANALYZE)
	{
		appendStringInfoString(vacuumPrefix, "ANALYZE,");
	}

#if (PG_VERSION_NUM >= 90600)
	if (vacuumFlags & VACOPT_DISABLE_PAGE_SKIPPING)
	{
		appendStringInfoString(vacuumPrefix, "DISABLE_PAGE_SKIPPING,");
	}
#endif

	if (vacuumFlags & VACOPT_FREEZE)
	{
		appendStringInfoString(vacuumPrefix, "FREEZE,");
	}

	if (vacuumFlags & VACOPT_FULL)
	{
		appendStringInfoString(vacuumPrefix, "FULL,");
	}

	vacuumPrefix->data[vacuumPrefix->len - 1] = ')';

	appendStringInfoChar(vacuumPrefix, ' ');

	return vacuumPrefix;
}


/*
 * DeparseVacuumColumnNames joins the list of strings using commas as a
 * delimiter. The whole thing is placed in parenthesis and set off with a
 * single space in order to facilitate appending it to the end of any VACUUM
 * or ANALYZE command which uses explicit column names. If the provided list
 * is empty, this function returns an empty string to keep the calling code
 * simplest.
 */
static char *
DeparseVacuumColumnNames(List *columnNameList)
{
	StringInfo columnNames = makeStringInfo();
	ListCell *columnNameCell = NULL;

	if (columnNameList == NIL)
	{
		return columnNames->data;
	}

	appendStringInfoString(columnNames, " (");

	foreach(columnNameCell, columnNameList)
	{
		char *columnName = strVal(lfirst(columnNameCell));

		appendStringInfo(columnNames, "%s,", columnName);
	}

	columnNames->data[columnNames->len - 1] = ')';

	return columnNames->data;
}


/*
 * ErrorIfUnsupportedIndexStmt checks if the corresponding index statement is
 * supported for distributed tables and errors out if it is not.
 */
static void
ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement)
{
	char *indexRelationName = createIndexStatement->idxname;
	if (indexRelationName == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("creating index without a name on a distributed table is "
							   "currently unsupported")));
	}

	if (createIndexStatement->tableSpace != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("specifying tablespaces with CREATE INDEX statements is "
							   "currently unsupported")));
	}

	if (createIndexStatement->concurrent)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("creating indexes concurrently on distributed tables is "
							   "currently unsupported")));
	}

	if (createIndexStatement->unique)
	{
		RangeVar *relation = createIndexStatement->relation;
		bool missingOk = false;

		/* caller uses ShareLock for non-concurrent indexes, use the same lock here */
		LOCKMODE lockMode = ShareLock;
		Oid relationId = RangeVarGetRelid(relation, lockMode, missingOk);
		Var *partitionKey = PartitionKey(relationId);
		char partitionMethod = PartitionMethod(relationId);
		List *indexParameterList = NIL;
		ListCell *indexParameterCell = NULL;
		bool indexContainsPartitionColumn = false;

		/*
		 * Reference tables do not have partition key, and unique constraints
		 * are allowed for them. Thus, we added a short-circuit for reference tables.
		 */
		if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			return;
		}

		if (partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on append-partitioned tables "
								   "is currently unsupported")));
		}

		indexParameterList = createIndexStatement->indexParams;
		foreach(indexParameterCell, indexParameterList)
		{
			IndexElem *indexElement = (IndexElem *) lfirst(indexParameterCell);
			char *columnName = indexElement->name;
			AttrNumber attributeNumber = InvalidAttrNumber;

			/* column name is null for index expressions, skip it */
			if (columnName == NULL)
			{
				continue;
			}

			attributeNumber = get_attnum(relationId, columnName);
			if (attributeNumber == partitionKey->varattno)
			{
				indexContainsPartitionColumn = true;
			}
		}

		if (!indexContainsPartitionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on non-partition "
								   "columns is currently unsupported")));
		}
	}
}


/*
 * ErrorIfUnsupportedDropIndexStmt checks if the corresponding drop index statement is
 * supported for distributed tables and errors out if it is not.
 */
static void
ErrorIfUnsupportedDropIndexStmt(DropStmt *dropIndexStatement)
{
	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	if (list_length(dropIndexStatement->objects) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot drop multiple distributed objects in a "
							   "single command"),
						errhint("Try dropping each object in a separate DROP "
								"command.")));
	}

	if (dropIndexStatement->concurrent)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("dropping indexes concurrently on distributed tables is "
							   "currently unsupported")));
	}
}


/*
 * ErrorIfUnsupportedAlterTableStmt checks if the corresponding alter table statement
 * is supported for distributed tables and errors out if it is not. Currently,
 * only the following commands are supported.
 *
 * ALTER TABLE ADD|DROP COLUMN
 * ALTER TABLE ALTER COLUMN SET DATA TYPE
 * ALTER TABLE SET|DROP NOT NULL
 * ALTER TABLE SET|DROP DEFAULT
 * ALTER TABLE ADD|DROP CONSTRAINT FOREIGN
 */
static void
ErrorIfUnsupportedAlterTableStmt(AlterTableStmt *alterTableStatement)
{
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	/* error out if any of the subcommands are unsupported */
	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = command->subtype;

		switch (alterTableType)
		{
			case AT_AddColumn:
			{
				if (IsA(command->def, ColumnDef))
				{
					ColumnDef *column = (ColumnDef *) command->def;

					/*
					 * Check for SERIAL pseudo-types. The structure of this
					 * check is copied from transformColumnDefinition.
					 */
					if (column->typeName && list_length(column->typeName->names) == 1 &&
						!column->typeName->pct_type)
					{
						char *typeName = strVal(linitial(column->typeName->names));

						if (strcmp(typeName, "smallserial") == 0 ||
							strcmp(typeName, "serial2") == 0 ||
							strcmp(typeName, "serial") == 0 ||
							strcmp(typeName, "serial4") == 0 ||
							strcmp(typeName, "bigserial") == 0 ||
							strcmp(typeName, "serial8") == 0)
						{
							ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											errmsg("cannot execute ADD COLUMN commands "
												   "involving serial pseudotypes")));
						}
					}
				}

				break;
			}

			case AT_DropColumn:
			case AT_ColumnDefault:
			case AT_AlterColumnType:
			case AT_SetNotNull:
			case AT_DropNotNull:
			{
				/* error out if the alter table command is on the partition column */

				Var *partitionColumn = NULL;
				HeapTuple tuple = NULL;
				char *alterColumnName = command->name;

				LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
				Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
				if (!OidIsValid(relationId))
				{
					continue;
				}

				partitionColumn = PartitionKey(relationId);

				tuple = SearchSysCacheAttName(relationId, alterColumnName);
				if (HeapTupleIsValid(tuple))
				{
					Form_pg_attribute targetAttr = (Form_pg_attribute) GETSTRUCT(tuple);

					/* reference tables do not have partition column, so allow them */
					if (partitionColumn != NULL &&
						targetAttr->attnum == partitionColumn->varattno)
					{
						ereport(ERROR, (errmsg("cannot execute ALTER TABLE command "
											   "involving partition column")));
					}

					ReleaseSysCache(tuple);
				}

				break;
			}

			case AT_AddConstraint:
			{
				Constraint *constraint = (Constraint *) command->def;
				LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
				Oid referencingTableId = InvalidOid;
				Oid referencedTableId = InvalidOid;
				Var *referencingTablePartitionColumn = NULL;
				Var *referencedTablePartitionColumn = NULL;
				ListCell *referencingTableAttr = NULL;
				ListCell *referencedTableAttr = NULL;
				bool foreignConstraintOnPartitionColumn = false;

				/* we only allow adding foreign constraints with ALTER TABLE */
				if (constraint->contype != CONSTR_FOREIGN)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create constraint"),
									errdetail("Citus cannot execute ADD CONSTRAINT "
											  "command other than ADD CONSTRAINT FOREIGN "
											  "KEY.")));
				}

				/* we only allow foreign constraints if they are only subcommand */
				if (commandList->length > 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Citus cannot execute ADD CONSTRAINT "
											  "FOREIGN KEY command together with other "
											  "subcommands."),
									errhint("You can issue each subcommand separately")));
				}

				referencingTableId = RangeVarGetRelid(alterTableStatement->relation,
													  lockmode,
													  alterTableStatement->missing_ok);
				referencedTableId = RangeVarGetRelid(constraint->pktable, lockmode,
													 alterTableStatement->missing_ok);

				/* we do not support foreign keys for reference tables */
				if (PartitionMethod(referencingTableId) == DISTRIBUTE_BY_NONE ||
					PartitionMethod(referencedTableId) == DISTRIBUTE_BY_NONE)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail(
										"Foreign key constraints are not allowed from or "
										"to reference tables.")));
				}

				/*
				 * ON DELETE SET NULL and ON DELETE SET DEFAULT is not supported. Because
				 * we do not want to set partition column to NULL or default value.
				 */
				if (constraint->fk_del_action == FKCONSTR_ACTION_SETNULL ||
					constraint->fk_del_action == FKCONSTR_ACTION_SETDEFAULT)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("SET NULL or SET DEFAULT is not supported"
											  " in ON DELETE operation.")));
				}

				/*
				 * ON UPDATE SET NULL, ON UPDATE SET DEFAULT and UPDATE CASCADE is not
				 * supported. Because we do not want to set partition column to NULL or
				 * default value. Also cascading update operation would require
				 * re-partitioning. Updating partition column value is not allowed anyway
				 * even outside of foreign key concept.
				 */
				if (constraint->fk_upd_action == FKCONSTR_ACTION_SETNULL ||
					constraint->fk_upd_action == FKCONSTR_ACTION_SETDEFAULT ||
					constraint->fk_upd_action == FKCONSTR_ACTION_CASCADE)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("SET NULL, SET DEFAULT or CASCADE is not"
											  " supported in ON UPDATE operation.")));
				}

				/*
				 * We will use constraint name in each placement by extending it at
				 * workers. Therefore we require it to be exist.
				 */
				if (constraint->conname == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Creating foreign constraint without a "
											  "name on a distributed table is currently "
											  "not supported.")));
				}

				/* to enforce foreign constraints, tables must be co-located */
				if (!TablesColocated(referencingTableId, referencedTableId))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Foreign key constraint can only be created"
											  " on co-located tables.")));
				}

				/*
				 * The following logic requires the referenced columns to exists in
				 * the statement. Otherwise, we cannot apply some of the checks.
				 */
				if (constraint->pk_attrs == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint "
										   "because referenced column list is empty"),
									errhint("Add column names to \"REFERENCES\" part of "
											"the statement.")));
				}

				/*
				 * Referencing column's list length should be equal to referenced columns
				 * list length.
				 */
				if (constraint->fk_attrs->length != constraint->pk_attrs->length)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Referencing column list and referenced "
											  "column list must be in same size.")));
				}

				/*
				 * Partition column must exist in both referencing and referenced side
				 * of the foreign key constraint. They also must be in same ordinal.
				 */
				referencingTablePartitionColumn = PartitionKey(referencingTableId);
				referencedTablePartitionColumn = PartitionKey(referencedTableId);

				/*
				 * We iterate over fk_attrs and pk_attrs together because partition
				 * column must be at the same place in both referencing and referenced
				 * side of the foreign key constraint
				 */
				forboth(referencingTableAttr, constraint->fk_attrs,
						referencedTableAttr, constraint->pk_attrs)
				{
					char *referencingAttrName = strVal(lfirst(referencingTableAttr));
					char *referencedAttrName = strVal(lfirst(referencedTableAttr));
					AttrNumber referencingAttrNo = get_attnum(referencingTableId,
															  referencingAttrName);
					AttrNumber referencedAttrNo = get_attnum(referencedTableId,
															 referencedAttrName);

					if (referencingTablePartitionColumn->varattno == referencingAttrNo &&
						referencedTablePartitionColumn->varattno == referencedAttrNo)
					{
						foreignConstraintOnPartitionColumn = true;
					}
				}

				if (!foreignConstraintOnPartitionColumn)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Partition column must exist both "
											  "referencing and referenced side of the "
											  "foreign constraint statement and it must "
											  "be in the same ordinal in both sides.")));
				}

				/*
				 * We do not allow to create foreign constraints if shard replication
				 * factor is greater than 1. Because in our current design, multiple
				 * replicas may cause locking problems and inconsistent shard contents.
				 */
				if (!SingleReplicatedTable(referencingTableId) ||
					!SingleReplicatedTable(referencedTableId))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot create foreign key constraint"),
									errdetail("Citus Community Edition currently "
											  "supports foreign key constraints only for "
											  "\"citus.shard_replication_factor = 1\"."),
									errhint("Please change "
											"\"citus.shard_replication_factor to 1\". To "
											"learn more about using foreign keys with "
											"other replication factors, please contact"
											" us at "
											"https://citusdata.com/about/contact_us.")));
				}

				break;
			}

			case AT_DropConstraint:
			{
				/* we will no perform any special check for ALTER TABLE DROP CONSTRAINT */
				break;
			}

			default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("alter table command is currently unsupported"),
								errdetail("Only ADD|DROP COLUMN, SET|DROP NOT NULL,"
										  " SET|DROP DEFAULT, ADD|DROP CONSTRAINT FOREIGN"
										  " KEY and TYPE subcommands are supported.")));
			}
		}
	}
}


/*
 * ErrorIfUnsupportedSeqStmt errors out if the provided create sequence
 * statement specifies a distributed table in its OWNED BY clause.
 */
static void
ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt)
{
	Oid ownedByTableId = InvalidOid;

	/* create is easy: just prohibit any distributed OWNED BY */
	if (OptionsSpecifyOwnedBy(createSeqStmt->options, &ownedByTableId))
	{
		if (IsDistributedTable(ownedByTableId))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create sequences that specify a distributed "
								   "table in their OWNED BY option"),
							errhint("Use a sequence in a distributed table by specifying "
									"a serial column type before creating any shards.")));
		}
	}
}


/*
 * ErrorIfDistributedAlterSeqOwnedBy errors out if the provided alter sequence
 * statement attempts to change the owned by property of a distributed sequence
 * or attempt to change a local sequence to be owned by a distributed table.
 */
static void
ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt)
{
	Oid sequenceId = RangeVarGetRelid(alterSeqStmt->sequence, AccessShareLock,
									  alterSeqStmt->missing_ok);
	Oid ownedByTableId = InvalidOid;
	Oid newOwnedByTableId = InvalidOid;
	int32 ownedByColumnId = 0;
	bool hasDistributedOwner = false;

	/* alter statement referenced nonexistent sequence; return */
	if (sequenceId == InvalidOid)
	{
		return;
	}

	/* see whether the sequences is already owned by a distributed table */
	if (sequenceIsOwned(sequenceId, &ownedByTableId, &ownedByColumnId))
	{
		hasDistributedOwner = IsDistributedTable(ownedByTableId);
	}

	if (OptionsSpecifyOwnedBy(alterSeqStmt->options, &newOwnedByTableId))
	{
		/* if a distributed sequence tries to change owner, error */
		if (hasDistributedOwner && ownedByTableId != newOwnedByTableId)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot alter OWNED BY option of a sequence "
								   "already owned by a distributed table")));
		}
		else if (!hasDistributedOwner && IsDistributedTable(newOwnedByTableId))
		{
			/* and don't let local sequences get a distributed OWNED BY */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot associate an existing sequence with a "
								   "distributed table"),
							errhint("Use a sequence in a distributed table by specifying "
									"a serial column type before creating any shards.")));
		}
	}
}


/*
 * ErrorIfUnsupportedTruncateStmt errors out if the command attempts to
 * truncate a distributed foreign table.
 */
static void
ErrorIfUnsupportedTruncateStmt(TruncateStmt *truncateStatement)
{
	List *relationList = truncateStatement->relations;
	ListCell *relationCell = NULL;
	foreach(relationCell, relationList)
	{
		RangeVar *rangeVar = (RangeVar *) lfirst(relationCell);
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, true);
		char relationKind = get_rel_relkind(relationId);
		if (IsDistributedTable(relationId) &&
			relationKind == RELKIND_FOREIGN_TABLE)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("truncating distributed foreign tables is "
								   "currently unsupported"),
							errhint("Use master_drop_all_shards to remove "
									"foreign table's shards.")));
		}
	}
}


/*
 * OptionsSpecifyOwnedBy processes the options list of either a CREATE or ALTER
 * SEQUENCE command, extracting the first OWNED BY option it encounters. The
 * identifier for the specified table is placed in the Oid out parameter before
 * returning true. Returns false if no such option is found. Still returns true
 * for OWNED BY NONE, but leaves the out paramter set to InvalidOid.
 */
static bool
OptionsSpecifyOwnedBy(List *optionList, Oid *ownedByTableId)
{
	ListCell *optionCell = NULL;

	foreach(optionCell, optionList)
	{
		DefElem *defElem = (DefElem *) lfirst(optionCell);
		if (strcmp(defElem->defname, "owned_by") == 0)
		{
			List *ownedByNames = defGetQualifiedName(defElem);
			int nameCount = list_length(ownedByNames);

			/* if only one name is present, this is OWNED BY NONE */
			if (nameCount == 1)
			{
				*ownedByTableId = InvalidOid;
				return true;
			}
			else
			{
				/*
				 * Otherwise, we have a list of schema, table, column, which we
				 * need to truncate to simply the schema and table to determine
				 * the relevant relation identifier.
				 */
				List *relNameList = list_truncate(list_copy(ownedByNames), nameCount - 1);
				RangeVar *rangeVar = makeRangeVarFromNameList(relNameList);
				bool failOK = true;

				*ownedByTableId = RangeVarGetRelid(rangeVar, NoLock, failOK);
				return true;
			}
		}
	}

	return false;
}


/*
 * ErrorIfDistributedRenameStmt errors out if the corresponding rename statement
 * operates on a distributed table or its objects.
 *
 * Note: This function handles only those rename statements which operate on tables.
 */
static void
ErrorIfDistributedRenameStmt(RenameStmt *renameStatement)
{
	Oid relationId = InvalidOid;
	bool isDistributedRelation = false;

	Assert(IsAlterTableRenameStmt(renameStatement));

	/*
	 * The lock levels here should be same as the ones taken in
	 * RenameRelation(), renameatt() and RenameConstraint(). However, since all
	 * three statements have identical lock levels, we just use a single statement.
	 */
	relationId = RangeVarGetRelid(renameStatement->relation, AccessExclusiveLock,
								  renameStatement->missing_ok);

	/*
	 * If the table does not exist, we don't do anything here, and allow postgres to
	 * throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(relationId))
	{
		return;
	}

	isDistributedRelation = IsDistributedTable(relationId);
	if (isDistributedRelation)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("renaming distributed tables or their objects is "
							   "currently unsupported")));
	}
}


/*
 * CreateLocalTable gets DDL commands from the remote node for the given
 * relation. Then, it creates the local relation as temporary and on commit drop.
 */
static void
CreateLocalTable(RangeVar *relation, char *nodeName, int32 nodePort)
{
	List *ddlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedRelationName = quote_qualified_identifier(schemaName, relationName);

	/*
	 * The warning message created in TableDDLCommandList() is descriptive
	 * enough; therefore, we just throw an error which says that we could not
	 * run the copy operation.
	 */
	ddlCommandList = TableDDLCommandList(nodeName, nodePort, qualifiedRelationName);
	if (ddlCommandList == NIL)
	{
		ereport(ERROR, (errmsg("could not run copy from the worker node")));
	}

	/* apply DDL commands against the local database */
	foreach(ddlCommandCell, ddlCommandList)
	{
		StringInfo ddlCommand = (StringInfo) lfirst(ddlCommandCell);
		Node *ddlCommandNode = ParseTreeNode(ddlCommand->data);
		bool applyDDLCommand = false;

		if (IsA(ddlCommandNode, CreateStmt) ||
			IsA(ddlCommandNode, CreateForeignTableStmt))
		{
			CreateStmt *createStatement = (CreateStmt *) ddlCommandNode;

			/* create the local relation as temporary and on commit drop */
			createStatement->relation->relpersistence = RELPERSISTENCE_TEMP;
			createStatement->oncommit = ONCOMMIT_DROP;

			/* temporarily strip schema name */
			createStatement->relation->schemaname = NULL;

			applyDDLCommand = true;
		}
		else if (IsA(ddlCommandNode, CreateForeignServerStmt))
		{
			CreateForeignServerStmt *createServerStmt =
				(CreateForeignServerStmt *) ddlCommandNode;
			if (GetForeignServerByName(createServerStmt->servername, true) == NULL)
			{
				/* create server if not exists */
				applyDDLCommand = true;
			}
		}
		else if ((IsA(ddlCommandNode, CreateExtensionStmt)))
		{
			applyDDLCommand = true;
		}
		else if ((IsA(ddlCommandNode, CreateSeqStmt)))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot copy to table with serial column from worker"),
							errhint("Connect to the master node to COPY to tables which "
									"use serial column types.")));
		}

		/* run only a selected set of DDL commands */
		if (applyDDLCommand)
		{
			ProcessUtility(ddlCommandNode, CreateCommandTag(ddlCommandNode),
						   PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);
			CommandCounterIncrement();
		}
	}
}


/*
 * IsAlterTableRenameStmt returns true if the passed in RenameStmt operates on a
 * distributed table or its objects. This includes:
 * ALTER TABLE RENAME
 * ALTER TABLE RENAME COLUMN
 * ALTER TABLE RENAME CONSTRAINT
 */
static bool
IsAlterTableRenameStmt(RenameStmt *renameStmt)
{
	bool isAlterTableRenameStmt = false;

	if (renameStmt->renameType == OBJECT_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_COLUMN &&
			 renameStmt->relationType == OBJECT_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_TABCONSTRAINT)
	{
		isAlterTableRenameStmt = true;
	}

	return isAlterTableRenameStmt;
}


/*
 * ExecuteDistributedDDLJob simply executes a provided DDLJob in a distributed
 * transaction, including metadata sync if needed. If the multi shard commit protocol is
 * in its default value of '1pc', then a notice message indicating that '2pc' might be
 * used for extra safety. In the commit protocol, a BEGIN is sent after connection to
 * each shard placement and COMMIT/ROLLBACK is handled by
 * CompleteShardPlacementTransactions function.
 */
static void
ExecuteDistributedDDLJob(DDLJob *ddlJob)
{
	bool shouldSyncMetadata = ShouldSyncTableMetadata(ddlJob->targetRelationId);

	if (XactModificationLevel == XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("distributed DDL commands must not appear within "
							   "transaction blocks containing single-shard data "
							   "modifications")));
	}

	EnsureCoordinator();
	ShowNoticeIfNotUsing2PC();

	if (shouldSyncMetadata)
	{
		SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);
		SendCommandToWorkers(WORKERS_WITH_METADATA, (char *) ddlJob->commandString);
	}

	ExecuteModifyTasksWithoutResults(ddlJob->taskList);
}


/*
 * ShowNoticeIfNotUsing2PC shows a notice message about using 2PC by setting
 * citus.multi_shard_commit_protocol to 2PC. The notice message is shown only once in a
 * session
 */
static void
ShowNoticeIfNotUsing2PC(void)
{
	if (MultiShardCommitProtocol != COMMIT_PROTOCOL_2PC && !warnedUserAbout2PC)
	{
		ereport(NOTICE, (errmsg("using one-phase commit for distributed DDL commands"),
						 errhint("You can enable two-phase commit for extra safety with: "
								 "SET citus.multi_shard_commit_protocol TO '2pc'")));

		warnedUserAbout2PC = true;
	}
}


/*
 * DDLTaskList builds a list of tasks to execute a DDL command on a
 * given list of shards.
 */
static List *
DDLTaskList(Oid relationId, const char *commandString)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	ListCell *shardIntervalCell = NULL;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	char *escapedCommandString = quote_literal_cstr(commandString);
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		StringInfo applyCommand = makeStringInfo();
		Task *task = NULL;

		/*
		 * If rightRelationId is not InvalidOid, instead of worker_apply_shard_ddl_command
		 * we use worker_apply_inter_shard_ddl_command.
		 */
		appendStringInfo(applyCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
						 escapedSchemaName, escapedCommandString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = applyCommand->data;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * ForeignKeyTaskList builds a list of tasks to execute a foreign key command on a
 * shards of given list of distributed table.
 *
 * leftRelationId is the relation id of actual distributed table which given foreign key
 * command is applied. rightRelationId is the relation id of distributed table which
 * foreign key refers to.
 */
static List *
ForeignKeyTaskList(Oid leftRelationId, Oid rightRelationId,
				   const char *commandString)
{
	List *taskList = NIL;

	List *leftShardList = LoadShardIntervalList(leftRelationId);
	ListCell *leftShardCell = NULL;
	Oid leftSchemaId = get_rel_namespace(leftRelationId);
	char *leftSchemaName = get_namespace_name(leftSchemaId);
	char *escapedLeftSchemaName = quote_literal_cstr(leftSchemaName);

	List *rightShardList = LoadShardIntervalList(rightRelationId);
	ListCell *rightShardCell = NULL;
	Oid rightSchemaId = get_rel_namespace(rightRelationId);
	char *rightSchemaName = get_namespace_name(rightSchemaId);
	char *escapedRightSchemaName = quote_literal_cstr(rightSchemaName);

	char *escapedCommandString = quote_literal_cstr(commandString);
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	/* lock metadata before getting placement lists */
	LockShardListMetadata(leftShardList, ShareLock);

	forboth(leftShardCell, leftShardList, rightShardCell, rightShardList)
	{
		ShardInterval *leftShardInterval = (ShardInterval *) lfirst(leftShardCell);
		uint64 leftShardId = leftShardInterval->shardId;
		StringInfo applyCommand = makeStringInfo();
		Task *task = NULL;

		ShardInterval *rightShardInterval = (ShardInterval *) lfirst(rightShardCell);
		uint64 rightShardId = rightShardInterval->shardId;

		appendStringInfo(applyCommand, WORKER_APPLY_INTER_SHARD_DDL_COMMAND,
						 leftShardId, escapedLeftSchemaName, rightShardId,
						 escapedRightSchemaName, escapedCommandString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = applyCommand->data;
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = leftShardId;
		task->taskPlacementList = FinalizedShardPlacementList(leftShardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * Before acquiring a table lock, check whether we have sufficient rights.
 * In the case of DROP INDEX, also try to lock the table before the index.
 *
 * This code is heavily borrowed from RangeVarCallbackForDropRelation() in
 * commands/tablecmds.c in Postgres source. We need this to ensure the right
 * order of locking while dealing with DROP INDEX statments.
 */
static void
RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid, void *arg)
{
	/* *INDENT-OFF* */
	HeapTuple	tuple;
	struct DropRelationCallbackState *state;
	char		relkind;
	Form_pg_class classform;
	LOCKMODE	heap_lockmode;

	state = (struct DropRelationCallbackState *) arg;
	relkind = state->relkind;
	heap_lockmode = state->concurrent ?
		ShareUpdateExclusiveLock : AccessExclusiveLock;

	Assert(relkind == RELKIND_INDEX);

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relOid != oldRelOid && OidIsValid(state->heapOid))
	{
		UnlockRelationOid(state->heapOid, heap_lockmode);
		state->heapOid = InvalidOid;
	}

	/* Didn't find a relation, so no need for locking or permission checks. */
	if (!OidIsValid(relOid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped, so nothing to do */
	classform = (Form_pg_class) GETSTRUCT(tuple);

	if (classform->relkind != relkind)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not an index", rel->relname)));

	/* Allow DROP to either table owner or schema owner */
	if (!pg_class_ownercheck(relOid, GetUserId()) &&
		!pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   rel->relname);

	if (!allowSystemTableMods && IsSystemClass(relOid, classform))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						rel->relname)));

	ReleaseSysCache(tuple);

	/*
	 * In DROP INDEX, attempt to acquire lock on the parent table before
	 * locking the index.  index_drop() will need this anyway, and since
	 * regular queries lock tables before their indexes, we risk deadlock if
	 * we do it the other way around.  No error if we don't find a pg_index
	 * entry, though --- the relation may have been dropped.
	 */
	if (relkind == RELKIND_INDEX && relOid != oldRelOid)
	{
		state->heapOid = IndexGetRelation(relOid, true);
		if (OidIsValid(state->heapOid))
			LockRelationOid(state->heapOid, heap_lockmode);
	}
	/* *INDENT-ON* */
}


/*
 * Check whether the current user has the permission to execute a COPY
 * statement, raise ERROR if not. In some cases we have to do this separately
 * from postgres' copy.c, because we have to execute the copy with elevated
 * privileges.
 *
 * Copied from postgres, where it's part of DoCopy().
 */
static void
CheckCopyPermissions(CopyStmt *copyStatement)
{
	/* *INDENT-OFF* */
	bool		is_from = copyStatement->is_from;
	Relation	rel;
	Oid			relid;
	List	   *range_table = NIL;
	TupleDesc	tupDesc;
	AclMode		required_access = (is_from ? ACL_INSERT : ACL_SELECT);
	List	   *attnums;
	ListCell   *cur;
	RangeTblEntry *rte;

	rel = heap_openrv(copyStatement->relation,
								 is_from ? RowExclusiveLock : AccessShareLock);

	relid = RelationGetRelid(rel);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = relid;
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = required_access;
	range_table = list_make1(rte);

	tupDesc = RelationGetDescr(rel);

	attnums = CopyGetAttnums(tupDesc, rel, copyStatement->attlist);
	foreach(cur, attnums)
	{
		int			attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

		if (is_from)
		{
			rte->insertedCols = bms_add_member(rte->insertedCols, attno);
		}
		else
		{
			rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
	}

	ExecCheckRTPerms(range_table, true);

	/* TODO: Perform RLS checks once supported */

	heap_close(rel, NoLock);
	/* *INDENT-ON* */
}


/* Helper for CheckCopyPermissions(), copied from postgres */
static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	/* *INDENT-OFF* */
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		Form_pg_attribute *attr = tupDesc->attrs;
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
			{
				continue;
			}
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				if (tupDesc->attrs[i]->attisdropped)
				{
					continue;
				}
				if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0)
				{
					attnum = tupDesc->attrs[i]->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   name, RelationGetRelationName(rel))));
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
				}
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			}
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
	/* *INDENT-ON* */
}


/*
 * ReplicateGrantStmt replicates GRANT/REVOKE command to worker nodes if the
 * the statement affects distributed tables.
 *
 * NB: So far column level privileges are not supported.
 */
void
ReplicateGrantStmt(Node *parsetree)
{
	GrantStmt *grantStmt = (GrantStmt *) parsetree;
	StringInfoData privsString;
	StringInfoData granteesString;
	StringInfoData targetString;
	StringInfoData ddlString;
	ListCell *granteeCell = NULL;
	ListCell *objectCell = NULL;
	bool isFirst = true;
	DDLJob *ddlJob = palloc(sizeof(DDLJob));

	initStringInfo(&privsString);
	initStringInfo(&granteesString);
	initStringInfo(&targetString);
	initStringInfo(&ddlString);

	/*
	 * So far only table level grants are supported. Most other types of
	 * grants aren't interesting anyway.
	 */
	if (grantStmt->targtype != ACL_TARGET_OBJECT ||
		grantStmt->objtype != ACL_OBJECT_RELATION)
	{
		return;
	}

	/* deparse the privileges */
	if (grantStmt->privileges == NIL)
	{
		appendStringInfo(&privsString, "ALL");
	}
	else
	{
		ListCell *privilegeCell = NULL;

		isFirst = true;
		foreach(privilegeCell, grantStmt->privileges)
		{
			AccessPriv *priv = lfirst(privilegeCell);

			if (!isFirst)
			{
				appendStringInfoString(&privsString, ", ");
			}
			isFirst = false;

			Assert(priv->cols == NIL);
			Assert(priv->priv_name != NULL);

			appendStringInfo(&privsString, "%s", priv->priv_name);
		}
	}

	/* deparse the privileges */
	isFirst = true;
	foreach(granteeCell, grantStmt->grantees)
	{
		RoleSpec *spec = lfirst(granteeCell);

		if (!isFirst)
		{
			appendStringInfoString(&granteesString, ", ");
		}
		isFirst = false;

		if (spec->roletype == ROLESPEC_CSTRING)
		{
			appendStringInfoString(&granteesString, quote_identifier(spec->rolename));
		}
		else if (spec->roletype == ROLESPEC_CURRENT_USER)
		{
			appendStringInfoString(&granteesString, "CURRENT_USER");
		}
		else if (spec->roletype == ROLESPEC_SESSION_USER)
		{
			appendStringInfoString(&granteesString, "SESSION_USER");
		}
		else if (spec->roletype == ROLESPEC_PUBLIC)
		{
			appendStringInfoString(&granteesString, "PUBLIC");
		}
	}

	/*
	 * Deparse the target objects, and issue the deparsed statements to
	 * workers, if applicable. That's so we easily can replicate statements
	 * only to distributed relations.
	 */
	isFirst = true;
	foreach(objectCell, grantStmt->objects)
	{
		RangeVar *relvar = (RangeVar *) lfirst(objectCell);
		Oid relOid = RangeVarGetRelid(relvar, NoLock, false);
		const char *grantOption = "";

		if (!IsDistributedTable(relOid))
		{
			continue;
		}

		resetStringInfo(&targetString);
		appendStringInfo(&targetString, "%s", generate_relation_name(relOid, NIL));

		if (grantStmt->is_grant)
		{
			if (grantStmt->grant_option)
			{
				grantOption = " WITH GRANT OPTION";
			}

			appendStringInfo(&ddlString, "GRANT %s ON %s TO %s%s",
							 privsString.data, targetString.data, granteesString.data,
							 grantOption);
		}
		else
		{
			if (grantStmt->grant_option)
			{
				grantOption = "GRANT OPTION FOR ";
			}

			appendStringInfo(&ddlString, "REVOKE %s%s ON %s FROM %s",
							 grantOption, privsString.data, targetString.data,
							 granteesString.data);
		}

		ddlJob->targetRelationId = relOid;
		ddlJob->commandString = ddlString.data;
		ddlJob->taskList = DDLTaskList(relOid, ddlString.data);

		ExecuteDistributedDDLJob(ddlJob);

		resetStringInfo(&ddlString);
	}
}
