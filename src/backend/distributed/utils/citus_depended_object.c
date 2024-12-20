/*
 * citus_depended_object.c
 *
 * Implements exposed functions related to hiding citus depended objects.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "storage/large_object.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/citus_depended_object.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shared_library_init.h"

/*
 * GUC hides any objects, which depends on citus extension, from pg meta class queries,
 * it is intended to be used in vanilla tests to not break postgres test logs
 */
bool HideCitusDependentObjects = false;

static Node * CreateCitusDependentObjectExpr(int pgMetaTableVarno, int pgMetaTableOid);
static List * GetCitusDependedObjectArgs(int pgMetaTableVarno, int pgMetaTableOid);
static bool AlterRoleSetStatementContainsAll(Node *node);
static bool HasDropCommandViolatesOwnership(Node *node);
static bool AnyObjectViolatesOwnership(DropStmt *dropStmt);

/*
 * IsPgLocksTable returns true if RTE is pg_locks table.
 */
bool
IsPgLocksTable(RangeTblEntry *rte)
{
	Oid pgLocksId = get_relname_relid("pg_locks", get_namespace_oid("pg_catalog", false));
	return rte->relid == pgLocksId;
}


/*
 * SetLocalHideCitusDependentObjectsDisabledWhenAlreadyEnabled disables the GUC HideCitusDependentObjects
 * if only it is enabled for local transaction.
 */
void
SetLocalHideCitusDependentObjectsDisabledWhenAlreadyEnabled(void)
{
	if (!HideCitusDependentObjects)
	{
		return;
	}

	set_config_option("citus.hide_citus_dependent_objects", "false",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


/*
 * SetLocalClientMinMessagesIfRunningPGTests sets client_min_message locally to the given value
 * if EnableUnsupportedFeatureMessages is set to false.
 */
void
SetLocalClientMinMessagesIfRunningPGTests(int clientMinMessageLevel)
{
	if (EnableUnsupportedFeatureMessages)
	{
		return;
	}

	const char *clientMinMessageLevelName = GetClientMinMessageLevelNameForValue(
		clientMinMessageLevel);

	set_config_option("client_min_messages", clientMinMessageLevelName,
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


/*
 * HideCitusDependentObjectsOnQueriesOfPgMetaTables adds a NOT is_citus_depended_object(oid, oid) expr
 * to the quals of meta class RTEs that we are interested in.
 */
bool
HideCitusDependentObjectsOnQueriesOfPgMetaTables(Node *node, void *context)
{
	if (!CitusHasBeenLoaded() || !HideCitusDependentObjects || node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		MemoryContext queryContext = GetMemoryChunkContext(query);

		/*
		 * We process the whole rtable rather than visiting individual RangeTblEntry's
		 * in the walker, since we need to know the varno to generate the right
		 * filter.
		 */
		int varno = 0;
		RangeTblEntry *rangeTableEntry = NULL;

		foreach_declared_ptr(rangeTableEntry, query->rtable)
		{
			varno++;

			if (rangeTableEntry->rtekind == RTE_RELATION)
			{
				/* make sure the expression is in the right memory context */
				MemoryContext originalContext = MemoryContextSwitchTo(queryContext);

				Oid metaTableOid = InvalidOid;

				/*
				 * add NOT is_citus_depended_object(oid, oid) to the quals
				 * of the RTE if it is a pg meta table that we are interested in.
				 */
				switch (rangeTableEntry->relid)
				{
					/* pg_class */
					case RelationRelationId:

					/* pg_proc */
					case ProcedureRelationId:

					/* pg_am */
					case AccessMethodRelationId:

					/* pg_type */
					case TypeRelationId:

					/* pg_enum */
					case EnumRelationId:

					/* pg_event_trigger */
					case EventTriggerRelationId:

					/* pg_trigger */
					case TriggerRelationId:

					/* pg_rewrite */
					case RewriteRelationId:

					/* pg_attrdef */
					case AttrDefaultRelationId:

					/* pg_constraint */
					case ConstraintRelationId:

					/* pg_ts_config */
					case TSConfigRelationId:

					/* pg_ts_template */
					case TSTemplateRelationId:

					/* pg_ts_dict */
					case TSDictionaryRelationId:

					/* pg_language */
					case LanguageRelationId:

					/* pg_namespace */
					case NamespaceRelationId:

					/* pg_sequence */
					case SequenceRelationId:

					/* pg_statistic */
					case StatisticRelationId:

					/* pg_attribute */
					case AttributeRelationId:

					/* pg_index */
					case IndexRelationId:

					/* pg_operator */
					case OperatorRelationId:

					/* pg_opclass */
					case OperatorClassRelationId:

					/* pg_opfamily */
					case OperatorFamilyRelationId:

					/* pg_amop */
					case AccessMethodOperatorRelationId:

					/* pg_amproc */
					case AccessMethodProcedureRelationId:

					/* pg_aggregate */
					case AggregateRelationId:
					{
						metaTableOid = rangeTableEntry->relid;
						break;
					}

					default:
					{
						metaTableOid = InvalidOid;
						break;
					}
				}

				if (OidIsValid(metaTableOid))
				{
					bool mergeJoinCondition = false;
#if PG_VERSION_NUM >= PG_VERSION_17

					/*
					 * In Postgres 17, the query tree has a specific field for the merge condition.
					 * So we shouldn't modify the jointree, but rather the mergeJoinCondition here
					 * Relevant PG17 commit: 0294df2f1
					 */
					mergeJoinCondition = query->mergeJoinCondition;
#endif

					/*
					 * We found a valid pg meta class in query,
					 * so we assert below conditions.
					 */
					Assert(mergeJoinCondition ||
						   (query->jointree != NULL &&
							query->jointree->fromlist != NULL));

					Node *citusDependentObjExpr =
						CreateCitusDependentObjectExpr(varno, metaTableOid);

					/*
					 * We do not use security quals because a postgres vanilla test fails
					 * with a change of order for its result.
					 */
					if (!mergeJoinCondition)
					{
						query->jointree->quals = make_and_qual(
							query->jointree->quals, citusDependentObjExpr);
					}
					else
					{
#if PG_VERSION_NUM >= PG_VERSION_17
						query->mergeJoinCondition = make_and_qual(
							query->mergeJoinCondition, citusDependentObjExpr);
#endif
					}
				}

				MemoryContextSwitchTo(originalContext);
			}
		}

		return query_tree_walker((Query *) node,
								 HideCitusDependentObjectsOnQueriesOfPgMetaTables,
								 context, 0);
	}

	return expression_tree_walker(node, HideCitusDependentObjectsOnQueriesOfPgMetaTables,
								  context);
}


/*
 * CreateCitusDependentObjectExpr constructs an expression of the form:
 * NOT pg_catalog.is_citus_depended_object(oid, oid)
 */
static Node *
CreateCitusDependentObjectExpr(int pgMetaTableVarno, int pgMetaTableOid)
{
	/* build the call to read_intermediate_result */
	FuncExpr *funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = CitusDependentObjectFuncId();
	funcExpr->funcretset = false;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = GetCitusDependedObjectArgs(pgMetaTableVarno, pgMetaTableOid);

	BoolExpr *notExpr = makeNode(BoolExpr);
	notExpr->boolop = NOT_EXPR;
	notExpr->args = list_make1(funcExpr);
	notExpr->location = -1;

	return (Node *) notExpr;
}


/*
 * GetCitusDependedObjectArgs returns func arguments for pg_catalog.is_citus_depended_object
 */
static List *
GetCitusDependedObjectArgs(int pgMetaTableVarno, int pgMetaTableOid)
{
	/*
	 * set attribute number for the oid, which we are insterest in, inside pg meta tables.
	 * We are accessing the 1. col(their own oid or their relation's oid) to get the related
	 * object's oid for all of the pg meta tables except pg_enum and pg_index. For pg_enum,
	 * class, we access its 2. col(its type's oid) to see if its type depends on citus,
	 * so it does. For pg_index, we access its 2. col (its relation's oid) to see if its relation
	 * depends on citus, so it does.
	 */
	AttrNumber oidAttNum = 1;
	if (pgMetaTableOid == EnumRelationId || pgMetaTableOid == IndexRelationId)
	{
		oidAttNum = 2;
	}

	/* create const for meta table oid */
	Const *metaTableOidConst = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
										 ObjectIdGetDatum(pgMetaTableOid),
										 false, true);

	/*
	 * create a var for the oid that we are interested in,
	 * col type should be regproc for pg_aggregate table; else oid
	 */
	Oid varType = (pgMetaTableOid == AggregateRelationId) ? REGPROCOID : OIDOID;
	Var *oidVar = makeVar(pgMetaTableVarno, oidAttNum,
						  varType, -1, InvalidOid, 0);

	return list_make2((Node *) metaTableOidConst, (Node *) oidVar);
}


/*
 * DistOpsValidityState returns validation state for given dist ops.
 */
DistOpsValidationState
DistOpsValidityState(Node *node, const DistributeObjectOps *ops)
{
	if (ops && ops->operationType == DIST_OPS_CREATE)
	{
		/*
		 * We should not validate CREATE statements because no address exists
		 * here yet.
		 */
		return NoAddressResolutionRequired;
	}
	else if (AlterRoleSetStatementContainsAll(node))
	{
		/*
		 * We should not validate 'ALTER ROLE ALL [SET|UNSET] because for the role ALL
		 * AlterRoleSetStmtObjectAddress returns an invalid address even though it should not.
		 */
		return NoAddressResolutionRequired;
	}
	else if (HasDropCommandViolatesOwnership(node))
	{
		/*
		 * found object with an invalid ownership, PG will complain if there is any object
		 * with an invalid ownership.
		 */
		return HasObjectWithInvalidOwnership;
	}

	if (ops && ops->address)
	{
		bool missingOk = true;
		bool isPostprocess = false;
		List *objectAddresses = ops->address(node, missingOk, isPostprocess);
		ObjectAddress *objectAddress = NULL;
		foreach_declared_ptr(objectAddress, objectAddresses)
		{
			if (OidIsValid(objectAddress->objectId))
			{
				/* found one valid object */
				return HasAtLeastOneValidObject;
			}
		}

		/* no valid objects */
		return HasNoneValidObject;
	}
	else
	{
		/* if the object doesn't have address defined, we donot validate */
		return NoAddressResolutionRequired;
	}
}


/*
 * DistOpsInValidState returns true if given state is valid to execute
 * preprocess and qualify steps.
 */
bool
DistOpsInValidState(DistOpsValidationState distOpsValidationState)
{
	return distOpsValidationState == HasAtLeastOneValidObject || distOpsValidationState ==
		   NoAddressResolutionRequired;
}


/*
 * AlterRoleSetStatementContainsAll returns true if the statement is a
 * ALTER ROLE ALL (SET / RESET).
 */
static bool
AlterRoleSetStatementContainsAll(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (nodeTag(node) == T_AlterRoleSetStmt)
	{
		/* rolespec is null for the role 'ALL' */
		AlterRoleSetStmt *alterRoleSetStmt = castNode(AlterRoleSetStmt, node);

		return alterRoleSetStmt->role == NULL;
	}

	return false;
}


/*
 * HasDropCommandViolatesOwnership returns true if any object in the given
 * statement violates object ownership.
 *
 * Currently there is only one test which fails due to object ownership.
 * The command that is failing is DROP. If in the future we hit other
 * commands like this, we should expand this function.
 */
static bool
HasDropCommandViolatesOwnership(Node *node)
{
	if (!IsA(node, DropStmt))
	{
		return false;
	}

	DropStmt *dropStmt = castNode(DropStmt, node);
	if (AnyObjectViolatesOwnership(dropStmt))
	{
		return true;
	}

	return false;
}


/*
 * AnyObjectViolatesOwnership return true if given object in stmt violates ownership.
 */
static bool
AnyObjectViolatesOwnership(DropStmt *dropStmt)
{
	bool hasOwnershipViolation = false;
	ObjectAddress objectAddress = { 0 };
	volatile Relation relation = NULL;
	ObjectType objectType = dropStmt->removeType;
	bool missingOk = dropStmt->missing_ok;

	MemoryContext savedContext = CurrentMemoryContext;
	ResourceOwner savedOwner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(savedContext);

	PG_TRY();
	{
		Node *object = NULL;
		foreach_declared_ptr(object, dropStmt->objects)
		{
			Relation rel = NULL;
			objectAddress = get_object_address(objectType, object,
											   &rel, AccessShareLock, missingOk);

			/*
			 * The object relation is qualified with volatile and its value is obtained from
			 * get_object_address(). Unless we can qualify the corresponding parameter of
			 * get_object_address() with volatile (this is a function defined in PostgreSQL),
			 * we cannot get rid of this assignment.
			 */
			relation = rel;

			if (OidIsValid(objectAddress.objectId))
			{
				/*
				 * if object violates ownership, check_object_ownership will throw error.
				 */
				check_object_ownership(GetUserId(),
									   objectType,
									   objectAddress,
									   object, relation);
			}

			if (relation != NULL)
			{
				relation_close(relation, NoLock);
				relation = NULL;
			}
		}

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(savedContext);
		CurrentResourceOwner = savedOwner;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		hasOwnershipViolation = true;
		if (relation != NULL)
		{
			relation_close(relation, NoLock);
			relation = NULL;
		}
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(savedContext);
		CurrentResourceOwner = savedOwner;

		/* Rethrow error with LOG_SERVER_ONLY to prevent log to be sent to client */
		edata->elevel = LOG_SERVER_ONLY;
		ThrowErrorData(edata);
	}
	PG_END_TRY();

	return hasOwnershipViolation;
}
