/*-------------------------------------------------------------------------
 *
 * merge_planner.c
 *
 * This file contains functions to help plan MERGE queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <stddef.h>

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"

#include "pg_version_constants.h"

#include "distributed/citus_clauses.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_distributed_join_planner.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_node_metadata.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/repartition_executor.h"
#include "distributed/shard_pruning.h"
#include "distributed/shared_library_init.h"

static int SourceResultPartitionColumnIndex(Query *mergeQuery,
											List *sourceTargetList,
											CitusTableCacheEntry *targetRelation);
static Var * ValidateAndReturnVarIfSupported(Node *entryExpr);
static DeferredErrorMessage * DeferErrorIfTargetHasFalseClause(Oid targetRelationId,
															   PlannerRestrictionContext *
															   plannerRestrictionContext);
static void ErrorIfMergeQueryQualAndTargetListNotSupported(Oid targetRelationId,
														   Query *originalQuery);
static void ErrorIfMergeNotSupported(Query *query, Oid targetRelationId,
									 List *rangeTableList);
static void ErrorIfMergeHasUnsupportedTables(Oid targetRelationId, List *rangeTableList);
static bool IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
											  skipOuterVars);
static DeferredErrorMessage * DeferErrorIfRoutableMergeNotSupported(Query *query,
																	List *rangeTableList,
																	PlannerRestrictionContext
																	*
																	plannerRestrictionContext,
																	Oid targetRelationId);
static bool MergeSourceHasRouterSelect(Query *query,
									   PlannerRestrictionContext *
									   plannerRestrictionContext);
static DeferredErrorMessage * MergeQualAndTargetListFunctionsSupported(Oid
																	   resultRelationId,
																	   Query *query,
																	   Node *quals,
																	   List *targetList,
																	   CmdType commandType);

static DistributedPlan * CreateRouterMergePlan(Oid targetRelationId, Query *originalQuery,
											   Query *query,
											   List *rangeTableList,
											   PlannerRestrictionContext *
											   plannerRestrictionContext);
static void ErrorIfRepartitionMergeNotSupported(Oid targetRelationId, Query *mergeQuery,
												Query *sourceQuery);
static void ConvertSourceRTEIntoSubquery(Query *mergeQuery,
										 RangeTblEntry *sourceRte,
										 PlannerRestrictionContext *
										 plannerRestrictionContext);
static void ConvertSubqueryRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte);
static void ConvertCteRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte);
static void ConvertRelationRTEIntoSubquery(Query *mergeQuery,
										   RangeTblEntry *sourceRte,
										   PlannerRestrictionContext *
										   plannerRestrictionContext);
static void ErrorIfUnsupportedCTEs(Query *query);
static void ContainsUnsupportedCTEs(Query *query);
static bool MergeQueryCTEWalker(Node *node, void *context);
static DistributedPlan * CreateNonPushableMergePlan(Oid targetRelationId, uint64 planId,
													Query *originalQuery,
													Query *query,
													PlannerRestrictionContext *
													plannerRestrictionContext,
													ParamListInfo boundParams);
static char * MergeCommandResultIdPrefix(uint64 planId);
static void ErrorIfMergeHasReturningList(Query *query);
static Node * GetMergeJoinCondition(Query *mergeQuery);


/*
 * CreateMergePlan
 * 1) Check for conditions that are not supported in MERGE command.
 * 2) Try to create a pushable plan
 *    - Check for conditions suitable for a routable plan, if not found,
 *      raise deferred error
 * 3) Try to create repartition and redistribution plan
 *    - Check for conditions that prevent repartition strategy, if found,
 *      raise an exception and quit.
 */
DistributedPlan *
CreateMergePlan(uint64 planId, Query *originalQuery, Query *query,
				PlannerRestrictionContext *plannerRestrictionContext,
				ParamListInfo boundParams)
{
	Oid targetRelationId = ModifyQueryResultRelationId(originalQuery);

	/*
	 * Step 1: Look for definitive error conditions applicable to both Routable
	 * and Repartition strategies.
	 */
	List *rangeTableList = ExtractRangeTableEntryList(originalQuery);
	ErrorIfMergeNotSupported(originalQuery, targetRelationId, rangeTableList);

	/* Step 2: Try pushable merge plan */
	DistributedPlan *distributedPlan =
		CreateRouterMergePlan(targetRelationId, originalQuery, query,
							  rangeTableList, plannerRestrictionContext);

	/* Step 3: If the routing plan failed, try for repartition strategy */
	if (distributedPlan->planningError != NULL)
	{
		RaiseDeferredError(distributedPlan->planningError, DEBUG1);

		/* If MERGE is not routable, try repartitioning  */
		distributedPlan =
			CreateNonPushableMergePlan(targetRelationId, planId,
									   originalQuery, query,
									   plannerRestrictionContext,
									   boundParams);
	}

	return distributedPlan;
}


/*
 * GetMergeJoinTree constructs and returns the jointree for a MERGE query.
 */
FromExpr *
GetMergeJoinTree(Query *mergeQuery)
{
	FromExpr *mergeJointree = NULL;
#if PG_VERSION_NUM >= PG_VERSION_17

	/*
	 * In Postgres 17, the query tree has a specific field for the merge condition.
	 * For deriving the WhereClauseList from the merge condition, we construct a dummy
	 * jointree with an empty fromlist. This works because the fromlist of a merge query
	 * join tree consists of range table references only, and range table references are
	 * disregarded by the WhereClauseList() walker.
	 * Relevant PG17 commit: 0294df2f1
	 */
	mergeJointree = makeFromExpr(NIL, mergeQuery->mergeJoinCondition);
#else
	mergeJointree = mergeQuery->jointree;
#endif

	return mergeJointree;
}


/*
 * GetMergeJoinCondition returns the quals of the ON condition
 */
static Node *
GetMergeJoinCondition(Query *mergeQuery)
{
	Node *joinCondition = NULL;
#if PG_VERSION_NUM >= PG_VERSION_17
	joinCondition = (Node *) mergeQuery->mergeJoinCondition;
#else
	joinCondition = (Node *) mergeQuery->jointree->quals;
#endif
	return joinCondition;
}


/*
 * CreateRouterMergePlan attempts to create a pushable plan for the given MERGE
 * SQL statement. If the planning fails, the ->planningError is set to a description
 * of the failure.
 */
static DistributedPlan *
CreateRouterMergePlan(Oid targetRelationId, Query *originalQuery, Query *query,
					  List *rangeTableList,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	Assert(originalQuery->commandType == CMD_MERGE);
	Assert(OidIsValid(targetRelationId));

	distributedPlan->planningError = DeferErrorIfRoutableMergeNotSupported(originalQuery,
																		   rangeTableList,
																		   plannerRestrictionContext,
																		   targetRelationId);
	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}


	Job *job = RouterJob(originalQuery, plannerRestrictionContext,
						 &distributedPlan->planningError);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	ereport(DEBUG1, (errmsg("Creating MERGE router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->targetRelationId = targetRelationId;
	distributedPlan->modLevel = RowModifyLevelForQuery(query);

	/* There is no coordinator query for MERGE */
	distributedPlan->combineQuery = NULL;

	/* MERGE doesn't support RETURNING clause */
	distributedPlan->expectResults = false;
	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	return distributedPlan;
}


/*
 * CreateNonPushableMergePlan comes into effect if the router planning fails
 * and incorporates two planning strategies.
 *
 * ExecuteSourceAtWorkerAndRepartition(): Plan the source query independently,
 * execute the results into intermediate files, and repartition the files to
 * co-locate them with the merge-target table. Subsequently, compile a final
 * merge query on the target table using the intermediate results as the data
 * source.
 *
 * ExecuteSourceAtCoordAndRedistribution(): Execute the plan that requires
 * evaluation at the coordinator, run the query on the coordinator, and
 * redistribute the resulting rows to ensure colocation with the target shards.
 * Direct the MERGE SQL operation to the worker nodes' target shards, using the
 * intermediate files colocated with the data as the data source.
 */
static DistributedPlan *
CreateNonPushableMergePlan(Oid targetRelationId, uint64 planId, Query *originalQuery,
						   Query *query,
						   PlannerRestrictionContext *plannerRestrictionContext,
						   ParamListInfo boundParams)
{
	Query *mergeQuery = copyObject(originalQuery);
	RangeTblEntry *sourceRte = ExtractMergeSourceRangeTableEntry(mergeQuery, false);
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	ereport(DEBUG1, (errmsg("Creating MERGE repartition plan")));
	ConvertSourceRTEIntoSubquery(mergeQuery, sourceRte, plannerRestrictionContext);
	Query *sourceQuery = sourceRte->subquery;

	ErrorIfRepartitionMergeNotSupported(targetRelationId, mergeQuery, sourceQuery);

	CitusTableCacheEntry *targetRelation = GetCitusTableCacheEntry(targetRelationId);


	if (IsCitusTableType(targetRelation->relationId, SINGLE_SHARD_DISTRIBUTED))
	{
		/*
		 * if target table is SINGLE_SHARD_DISTRIBUTED let's set this to invalid -1
		 * so later in execution phase we don't rely on this value and try to find single shard of target instead.
		 */
		distributedPlan->sourceResultRepartitionColumnIndex = -1;
	}
	else
	{
		/*
		 * Get the index of the column in the source query that will be utilized
		 * to repartition the source rows, ensuring colocation with the target
		 */

		distributedPlan->sourceResultRepartitionColumnIndex =
			SourceResultPartitionColumnIndex(mergeQuery,
											 sourceQuery->targetList,
											 targetRelation);
	}

	/*
	 * Make a copy of the source query, since following code scribbles it
	 * but we need to keep the original for EXPLAIN.
	 */
	Query *sourceQueryCopy = copyObject(sourceQuery);

	/* plan the subquery, this may be another distributed query */
	int cursorOptions = CURSOR_OPT_PARALLEL_OK;
	PlannedStmt *sourceRowsPlan = pg_plan_query(sourceQueryCopy, NULL, cursorOptions,
												boundParams);
	bool isRepartitionAllowed = IsRedistributablePlan(sourceRowsPlan->planTree) &&
								IsSupportedRedistributionTarget(targetRelationId);

	/* If plan is distributed, no work at the coordinator */
	if (isRepartitionAllowed)
	{
		distributedPlan->modifyWithSelectMethod = MODIFY_WITH_SELECT_REPARTITION;
	}
	else
	{
		distributedPlan->modifyWithSelectMethod = MODIFY_WITH_SELECT_VIA_COORDINATOR;
	}

	/* There is no coordinator query for MERGE */
	distributedPlan->combineQuery = NULL;

	/* MERGE doesn't support RETURNING clause */
	distributedPlan->expectResults = false;

	distributedPlan->modLevel = RowModifyLevelForQuery(mergeQuery);
	distributedPlan->targetRelationId = targetRelationId;
	distributedPlan->intermediateResultIdPrefix = MergeCommandResultIdPrefix(planId);
	distributedPlan->modifyQueryViaCoordinatorOrRepartition = mergeQuery;
	distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition = sourceRowsPlan;
	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	return distributedPlan;
}


/*
 * ContainsUnsupportedCTEs checks the CTE if it's modifying or recursive CTE, if true,
 * raises an exception.
 */
static void
ContainsUnsupportedCTEs(Query *query)
{
	if (query->hasModifyingCTE)
	{
		ereport(ERROR, (errmsg("CTEs with modifying actions are not yet "
							   "supported in MERGE")));
	}

	if (query->hasRecursive)
	{
		ereport(ERROR, (errmsg("Recursive CTEs are not yet "
							   "supported in MERGE")));
	}
}


/*
 * MergeQueryCTEWalker descends into the MERGE query to check for any subqueries
 */
static bool
MergeQueryCTEWalker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		ContainsUnsupportedCTEs(query);

		query_tree_walker(query, MergeQueryCTEWalker, NULL, 0);

		/* we're done, no need to recurse anymore for this query */
		return false;
	}

	return expression_tree_walker(node, MergeQueryCTEWalker, context);
}


/*
 * ErrorIfUnsupportedCTEs checks for unsupported CTEs, such as, modifying and recursive
 */
static void
ErrorIfUnsupportedCTEs(Query *query)
{
	ContainsUnsupportedCTEs(query);
	query_tree_walker(query, MergeQueryCTEWalker, NULL, 0);
}


/*
 * ErrorIfMergeHasUnsupportedTables checks if all the tables(target, source or any CTE
 * present) in the MERGE command are local i.e. a combination of Citus local and Non-Citus
 * tables (regular Postgres tables), or distributed tables with some restrictions
 * raises an exception for all other combinations.
 */
static void
ErrorIfMergeHasUnsupportedTables(Oid targetRelationId, List *rangeTableList)
{
	RangeTblEntry *rangeTableEntry = NULL;
	foreach_declared_ptr(rangeTableEntry, rangeTableList)
	{
		Oid relationId = rangeTableEntry->relid;

		switch (rangeTableEntry->rtekind)
		{
			case RTE_RELATION:
			{
				/* Check the relation type */
				break;
			}

			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_JOIN:
			case RTE_CTE:
			{
				/* Skip them as base table(s) will be checked */
				continue;
			}

			/*
			 * RTE_NAMEDTUPLESTORE is typically used in ephmeral named relations,
			 * such as, trigger data; until we find a genuine use case, raise an
			 * exception.
			 * RTE_RESULT is a node added by the planner and we shouldn't
			 * encounter it in the parse tree.
			 */
			case RTE_NAMEDTUPLESTORE:
			case RTE_RESULT:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("MERGE command is not supported with "
									   "Tuplestores and results")));
				break;
			}

			default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg(
									"MERGE command: Unrecognized range table entry(%d) ",
									rangeTableEntry->rtekind)));
			}
		}

		/* RTE Relation can be of various types, check them now */
		switch (rangeTableEntry->relkind)
		{
			/* skip the regular views as they are replaced with subqueries */
			case RELKIND_VIEW:
			{
				continue;
			}

			case RELKIND_MATVIEW:
			case RELKIND_FOREIGN_TABLE:
			{
				/* These two cases as a target is not allowed */
				if (relationId == targetRelationId)
				{
					/* Usually we don't reach this exception as the Postgres parser catches it */
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("MERGE command is not allowed on "
										   "relation type(relkind:%c)",
										   rangeTableEntry->relkind)));
				}
				break;
			}

			case RELKIND_RELATION:
			case RELKIND_PARTITIONED_TABLE:
			{
				/* Check for citus/postgres table types */
				Assert(OidIsValid(relationId));
				break;
			}

			default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Unexpected table type(relkind:%c) "
									   "in MERGE command", rangeTableEntry->relkind)));
			}
		}

		/*
		 * Check for unsupported distributed tables
		 */
		if (extern_IsColumnarTableAmTable(relationId) &&
			relationId == targetRelationId)
		{
			/* Columnar tables are not supported */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Columnar table as target is "
								   "not allowed in MERGE command")));
		}
		else if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			/* Append/Range distributed tables are not supported */
			if (IsCitusTableType(relationId, APPEND_DISTRIBUTED) ||
				IsCitusTableType(relationId, RANGE_DISTRIBUTED))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("For MERGE command, append/range distribution "
									   "table is not supported yet")));
			}
		}
		else if (IsCitusTableType(relationId, REFERENCE_TABLE) &&
				 relationId == targetRelationId)
		{
			/* Reference table as a target is not allowed */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Reference table as target is "
								   "not allowed in MERGE command")));
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			/*
			 * All the tables are local/reference, supported as long as
			 * coordinator is in the metadata.
			 */
			if (FindCoordinatorNodeId() == -1)
			{
				ereport(ERROR, (errmsg("Coordinator node is not in "
									   "the metadata"),
								errhint("To ensure that the distributed planner "
										"planner the Citus table, please consider "
										"configuring a coordinator node")));
			}
		}
	}
}


/*
 * IsPartitionColumnInMerge returns true if the given column is a partition column.
 * The function uses FindReferencedTableColumn to find the original relation
 * id and column that the column expression refers to. It then checks whether
 * that column is a partition column of the relation.
 *
 * Also, the function returns always false for reference tables given that
 * reference tables do not have partition column.
 *
 * If skipOuterVars is true, then it doesn't process the outervars.
 */
bool
IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
								  skipOuterVars)
{
	bool isDistributionColumn = false;
	Var *column = NULL;
	RangeTblEntry *relationRTE = NULL;

	/* ParentQueryList is same as the original query for MERGE */
	FindReferencedTableColumn(columnExpression, list_make1(query), query, &column,
							  &relationRTE,
							  skipOuterVars);
	Oid relationId = relationRTE ? relationRTE->relid : InvalidOid;
	if (relationId != InvalidOid && column != NULL)
	{
		Var *distributionColumn = DistPartitionKey(relationId);

		/* not all distributed tables have partition column */
		if (distributionColumn != NULL && column->varattno ==
			distributionColumn->varattno)
		{
			isDistributionColumn = true;
		}
	}

	return isDistributionColumn;
}


/*
 * MergeQualAndTargetListFunctionsSupported Checks WHEN/ON clause actions to see what functions
 * are allowed, if we are updating distribution column, etc.
 */
static DeferredErrorMessage *
MergeQualAndTargetListFunctionsSupported(Oid resultRelationId, Query *query,
										 Node *quals,
										 List *targetList, CmdType commandType)
{
	uint32 targetRangeTableIndex = query->resultRelation;
	FromExpr *joinTree = GetMergeJoinTree(query);
	Var *distributionColumn = NULL;
	if (IsCitusTable(resultRelationId) && HasDistributionKey(resultRelationId))
	{
		distributionColumn = PartitionColumn(resultRelationId, targetRangeTableIndex);
	}

	ListCell *targetEntryCell = NULL;
	bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
	bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		bool targetEntryDistributionColumn = false;
		AttrNumber targetColumnAttrNumber = InvalidAttrNumber;

		if (distributionColumn)
		{
			if (commandType == CMD_UPDATE)
			{
				/*
				 * Note that it is not possible to give an alias to
				 * UPDATE table SET ...
				 */
				if (targetEntry->resname)
				{
					targetColumnAttrNumber = get_attnum(resultRelationId,
														targetEntry->resname);
					if (targetColumnAttrNumber == distributionColumn->varattno)
					{
						targetEntryDistributionColumn = true;
					}
				}
			}
		}

		if (targetEntryDistributionColumn &&
			TargetEntryChangesValue(targetEntry, distributionColumn, joinTree))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "updating the distribution column is not "
								 "allowed in MERGE actions",
								 NULL, NULL);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  CitusIsVolatileFunction))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "functions used in MERGE actions on distributed "
								 "tables must not be VOLATILE",
								 NULL, NULL);
		}

		if (MasterIrreducibleExpression((Node *) targetEntry->expr,
										&hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  NodeIsFieldStore))
		{
			/* DELETE cannot do field indirection already */
			Assert(commandType == CMD_UPDATE || commandType == CMD_INSERT);
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "inserting or modifying composite type fields is not "
								 "supported", NULL,
								 "Use the column name to insert or update the composite "
								 "type as a single value");
		}
	}


	/*
	 * Check the condition, convert list of expressions into expression tree for further processing
	 */
	if (quals)
	{
		if (IsA(quals, List))
		{
			quals = (Node *) make_ands_explicit((List *) quals);
		}

		if (FindNodeMatchingCheckFunction((Node *) quals, CitusIsVolatileFunction))
		{
			StringInfo errorMessage = makeStringInfo();
			appendStringInfo(errorMessage, "functions used in the %s clause of MERGE "
										   "queries on distributed tables must not be VOLATILE",
							 (commandType == CMD_MERGE) ? "ON" : "WHEN");
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 errorMessage->data, NULL, NULL);
		}
		else if (MasterIrreducibleExpression(quals, &hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}
	}

	if (hasVarArgument)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "STABLE functions used in MERGE queries "
							 "cannot be called with column references",
							 NULL, NULL);
	}

	if (hasBadCoalesce)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "non-IMMUTABLE functions are not allowed in CASE or "
							 "COALESCE statements",
							 NULL, NULL);
	}

	if (quals != NULL && nodeTag(quals) == T_CurrentOfExpr)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot run MERGE actions with cursors",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * RepartitionMergeSupported checks if certain conditions cannot accommodate the
 * strategy of repartition and redistribution of source rows, the routine will verify
 * them and subsequently raises an exception.
 */
static void
ErrorIfRepartitionMergeNotSupported(Oid targetRelationId, Query *mergeQuery,
									Query *sourceQuery)
{
	if (!IsCitusTableType(targetRelationId, DISTRIBUTED_TABLE))
	{
		ereport(ERROR,
				(errmsg("MERGE involving repartition of rows "
						"is supported only if the target is distributed")));
	}

	RTEListProperties *queryRteListProperties = GetRTEListPropertiesForQuery(mergeQuery);
	if (queryRteListProperties->hasPostgresLocalTable)
	{
		ereport(ERROR, (errmsg("MERGE INTO an distributed table from "
							   "Postgres table is not yet supported")));
	}

	queryRteListProperties = GetRTEListPropertiesForQuery(sourceQuery);
	if (!queryRteListProperties->hasCitusTable)
	{
		ereport(ERROR, (errmsg("To MERGE into a distributed table, source must "
							   "be Citus table(s)")));
	}

	/*
	 * Sub-queries and CTEs are not allowed in actions and ON clause
	 */
	Node *joinCondition = GetMergeJoinCondition(mergeQuery);

	if (FindNodeMatchingCheckFunction(joinCondition, IsNodeSubquery))
	{
		ereport(ERROR,
				(errmsg("Sub-queries and CTEs are not allowed in ON clause for MERGE "
						"with repartitioning"),
				 errhint("Consider making the source and target colocated "
						 "and joined on the distribution column to make it a "
						 "routable query")));
	}

	MergeAction *action = NULL;
	foreach_declared_ptr(action, mergeQuery->mergeActionList)
	{
		if (FindNodeMatchingCheckFunction((Node *) action, IsNodeSubquery))
		{
			ereport(ERROR,
					(errmsg("Sub-queries and CTEs are not allowed in actions for MERGE "
							"with repartitioning"),
					 errhint("Consider making the source and target colocated "
							 "and joined on the distribution column to make it a "
							 "routable query")));
		}
	}
}


/*
 * ConvertCteRTEIntoSubquery takes a RTE_CTE and converts it into a RTE_SUBQUERY.
 */
static void
ConvertCteRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte)
{
	CommonTableExpr *sourceCte = NULL;
	CommonTableExpr *candidateCte = NULL;
	List *cteList = NIL;

	/*
	 * Presently, CTEs are only permitted within the USING clause, and thus,
	 * we search for the corresponding one
	 */
	foreach_declared_ptr(candidateCte, mergeQuery->cteList)
	{
		if (strcmp(candidateCte->ctename, sourceRte->ctename) == 0)
		{
			/* The source CTE that will be converted to a subquery */
			sourceCte = candidateCte;
		}
		else
		{
			/*
			 * Save any other CTEs that are referenced, either directly
			 * or indirectly, in the source CTE.
			 */
			cteList = lappend(cteList, candidateCte);
		}
	}

	Assert(sourceCte);

	Query *cteQuery = (Query *) copyObject(sourceCte->ctequery);

	sourceRte->rtekind = RTE_SUBQUERY;
#if PG_VERSION_NUM >= PG_VERSION_16

	/* sanity check - sourceRte was RTE_CTE previously so it should have no perminfo */
	Assert(sourceRte->perminfoindex == 0);
#endif

	/*
	 * As we are delinking the CTE from main query, we have to walk through the
	 * tree and decrement the ctelevelsup, but by wrapping a subquery, we avoid
	 * adjusting the ctelevelsup in RTE's
	 */
	sourceRte->subquery = WrapSubquery(cteQuery);

	/* Copy the rest of the CTEs(if any) and remove them from main query */
	sourceRte->subquery->cteList = copyObject(cteList);
	mergeQuery->cteList = NIL;

	/* Zero out CTE-specific fields */
	sourceRte->security_barrier = false;
	sourceRte->ctename = NULL;
	sourceRte->ctelevelsup = 0;
	sourceRte->self_reference = false;
	sourceRte->coltypes = NIL;
	sourceRte->coltypmods = NIL;
	sourceRte->colcollations = NIL;
}


/*
 * ConvertRelationRTEIntoSubquery takes a RTE_RELATION and converts it into a RTE_SUBQUERY,
 * which is basically a SELECT * FROM the relation.
 */
static void
ConvertRelationRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte,
							   PlannerRestrictionContext *plannerRestrictionContext)
{
	Query *sourceResultsQuery = makeNode(Query);
	RangeTblRef *newRangeTableRef = makeNode(RangeTblRef);
	List *requiredAttributes = NIL;

	RelationRestriction *relationRestriction =
		RelationRestrictionForRelation(sourceRte, plannerRestrictionContext);
	if (relationRestriction)
	{
		requiredAttributes =
			RequiredAttrNumbersForRelationInternal(mergeQuery,
												   relationRestriction->index);
	}

	sourceResultsQuery->commandType = CMD_SELECT;

	/* we copy the input rteRelation to preserve the rteIdentity */
	RangeTblEntry *newRangeTableEntry = copyObject(sourceRte);
	sourceResultsQuery->rtable = list_make1(newRangeTableEntry);

#if PG_VERSION_NUM >= PG_VERSION_16
	sourceResultsQuery->rteperminfos = NIL;
	if (sourceRte->perminfoindex)
	{
		/* create permission info for newRangeTableEntry */
		RTEPermissionInfo *perminfo = getRTEPermissionInfo(mergeQuery->rteperminfos,
														   sourceRte);

		/* update the sourceResultsQuery's rteperminfos accordingly */
		newRangeTableEntry->perminfoindex = 1;
		sourceResultsQuery->rteperminfos = list_make1(perminfo);
	}
#endif

	/* set the FROM expression to the subquery */
	newRangeTableRef->rtindex = SINGLE_RTE_INDEX;
	sourceResultsQuery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);
	sourceResultsQuery->targetList =
		CreateFilteredTargetListForRelation(sourceRte->relid, requiredAttributes);
	List *restrictionList =
		GetRestrictInfoListForRelation(sourceRte, plannerRestrictionContext);
	List *copyRestrictionList = copyObject(restrictionList);
	Expr *andedBoundExpressions = make_ands_explicit(copyRestrictionList);
	sourceResultsQuery->jointree->quals = (Node *) andedBoundExpressions;

	/*
	 * Originally the quals were pointing to the RTE and its varno
	 * was pointing to its index in rtable. However now we converted the RTE
	 * to a subquery and the quals should be pointing to that subquery, which
	 * is the only RTE in its rtable, hence we update the varnos so that they
	 * point to the subquery RTE.
	 * Originally: rtable: [rte1, current_rte, rte3...]
	 * Now: rtable: [rte1, subquery[current_rte], rte3...] --subquery[current_rte] refers to its rtable.
	 */
	Node *quals = sourceResultsQuery->jointree->quals;
	UpdateVarNosInNode(quals, SINGLE_RTE_INDEX);

	/* replace the function with the constructed subquery */
	sourceRte->rtekind = RTE_SUBQUERY;
#if PG_VERSION_NUM >= PG_VERSION_16
	sourceRte->perminfoindex = 0;
#endif
	sourceRte->subquery = sourceResultsQuery;
	sourceRte->inh = false;
}


/*
 * ConvertSubqueryRTEIntoSubquery takes a RTE_SUBQUERY and wraps it into a new
 * subquery, which eliminates any resjunk columns and adjusts the CTE levelsup.
 * In addition, if the subquery happens to be a SET operation, such as,
 * (SELECT * from a UNION SELECT * FROM b), it reorders, adds casts  and
 * prepares a single taget list
 */
static void
ConvertSubqueryRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte)
{
	sourceRte->subquery = WrapSubquery(sourceRte->subquery);

	if (list_length(mergeQuery->cteList) > 0)
	{
		/* copy CTEs from the MERGE ... INTO statement into source subquery */
		sourceRte->subquery->cteList = copyObject(mergeQuery->cteList);
		sourceRte->subquery->hasModifyingCTE = mergeQuery->hasModifyingCTE;
		mergeQuery->cteList = NIL;
	}
}


/*
 * ConvertSourceRTEIntoSubquery converts MERGE's source RTE into a subquery,
 * whose result rows are repartitioned during runtime.
 */
static void
ConvertSourceRTEIntoSubquery(Query *mergeQuery, RangeTblEntry *sourceRte,
							 PlannerRestrictionContext *plannerRestrictionContext)
{
	switch (sourceRte->rtekind)
	{
		case RTE_SUBQUERY:
		{
			ConvertSubqueryRTEIntoSubquery(mergeQuery, sourceRte);
			return;
		}

		case RTE_RELATION:
		{
			ConvertRelationRTEIntoSubquery(mergeQuery,
										   sourceRte, plannerRestrictionContext);
			return;
		}

		case RTE_CTE:
		{
			ConvertCteRTEIntoSubquery(mergeQuery, sourceRte);
			return;
		}

		default:
		{
			ereport(ERROR, (errmsg("Currently, Citus only supports "
								   "table, subquery, and CTEs as "
								   "valid sources for the MERGE "
								   "operation")));
		}
	}
}


/*
 * ErrorIfMergeHasReturningList raises an exception if the MERGE has
 * a RETURNING clause, as we don't support this yet for Citus tables
 * Relevant PG17 commit: c649fa24a
 */
static void
ErrorIfMergeHasReturningList(Query *query)
{
	if (query->returningList)
	{
		ereport(ERROR, (errmsg("MERGE with RETURNING is not yet supported "
							   "for Citus tables")));
	}
}


/*
 * ErrorIfMergeNotSupported Checks for conditions that are not supported in either
 * the routable or repartition strategies. It checks for
 * - MERGE with a RETURNING clause
 * - Supported table types and their combinations
 * - Check the target lists and quals of both the query and merge actions
 * - Supported CTEs
 */
static void
ErrorIfMergeNotSupported(Query *query, Oid targetRelationId, List *rangeTableList)
{
	ErrorIfMergeHasReturningList(query);
	ErrorIfMergeHasUnsupportedTables(targetRelationId, rangeTableList);
	ErrorIfMergeQueryQualAndTargetListNotSupported(targetRelationId, query);
	ErrorIfUnsupportedCTEs(query);
}


/*
 * DeferErrorIfTargetHasFalseClause checks for the presence of a false clause in the
 * target relation and throws an exception if found. Router planner prunes all the shards
 * for relations with such clauses, resulting in no task generation for the job. However,
 * in the case of a MERGE query, tasks still need to be generated for the shards of the
 * source relation.
 */
static DeferredErrorMessage *
DeferErrorIfTargetHasFalseClause(Oid targetRelationId,
								 PlannerRestrictionContext *plannerRestrictionContext)
{
	ListCell *restrictionCell = NULL;
	foreach(restrictionCell,
			plannerRestrictionContext->relationRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;

		/* Check only for target relation */
		if (relationId != targetRelationId)
		{
			continue;
		}

		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		if (ContainsFalseClause(restrictClauseList) ||
			JoinConditionIsOnFalse(relationRestriction->relOptInfo->joininfo))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Routing query is not possible with "
								 "no shards for target", NULL, NULL);
		}
	}
	return NULL;
}


/*
 * DeferErrorIfRoutableMergeNotSupported Checks for conditions that prevent pushable planning, if
 * found, raises a deferred error, which then continues to try repartitioning strategy.
 */
static DeferredErrorMessage *
DeferErrorIfRoutableMergeNotSupported(Query *query, List *rangeTableList,
									  PlannerRestrictionContext *plannerRestrictionContext,
									  Oid targetRelationId)
{
	List *distTablesList = NIL;
	List *refTablesList = NIL;
	List *localTablesList = NIL;
	RangeTblEntry *rangeTableEntry = NULL;

	foreach_declared_ptr(rangeTableEntry, rangeTableList)
	{
		Oid relationId = rangeTableEntry->relid;

		if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			distTablesList = lappend(distTablesList, rangeTableEntry);
		}
		else if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			refTablesList = lappend(refTablesList, rangeTableEntry);
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			localTablesList = lappend(localTablesList, rangeTableEntry);
		}
	}

	if (list_length(distTablesList) > 0 && list_length(refTablesList) > 0)
	{
		ereport(DEBUG1, (errmsg(
							 "A mix of distributed and reference table, try repartitioning")));
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "A mix of distributed and reference table, "
							 "routable query is not possible", NULL, NULL);
	}

	if (list_length(distTablesList) > 0 && list_length(localTablesList) > 0)
	{
		ereport(DEBUG1, (errmsg(
							 "A mix of distributed and local table, try repartitioning")));
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "A mix of distributed and citus-local table, "
							 "routable query is not possible", NULL, NULL);
	}

	/*
	 * If all tables are either local or reference tables, no need to proceed further down
	 * as the below checks are applicable for distributed tables only
	 */
	if (list_length(distTablesList) == 0)
	{
		return NULL;
	}

	/* Only one distributed table is involved in the MERGE */
	if (list_length(distTablesList) == 1)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "There is only one distributed table, merge is not "
							 "pushable, try repartitioning", NULL, NULL);
	}

	/* Ensure all distributed tables are indeed co-located */
	if (!AllDistributedRelationsInRTEListColocated(distTablesList))
	{
		ereport(DEBUG1, (errmsg("Distributed tables are not co-located, try "
								"repartitioning")));
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, all the distributed tables "
							 "must be colocated", NULL, NULL);
	}

	DeferredErrorMessage *deferredError = NULL;


	/*
	 * if the query goes to a single node ("router" in Citus' parlance),
	 * we don't need to go through certain SQL support and colocation checks.
	 *
	 * For PG16+, this is required as some of the outer JOINs are converted to
	 * "ON(true)" and filters are pushed down to the table scans. As
	 * DeferErrorIfUnsupportedSubqueryPushdown rely on JOIN filters, it will fail to
	 * detect the router case. However, we can still detect it by checking if
	 * the query is a router query as the router query checks the filters on
	 * the tables.
	 */


	if (!MergeSourceHasRouterSelect(query, plannerRestrictionContext))
	{
		deferredError =
			DeferErrorIfUnsupportedSubqueryPushdown(query,
													plannerRestrictionContext);
		if (deferredError)
		{
			ereport(DEBUG1, (errmsg("Sub-query is not pushable, try repartitioning")));
			return deferredError;
		}

		if (HasDangerousJoinUsing(query->rtable, (Node *) query->jointree))
		{
			ereport(DEBUG1, (errmsg(
								 "Query has ambigious joins, merge is not pushable, try repartitioning")));
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "a join with USING causes an internal naming "
								 "conflict, use ON instead", NULL, NULL);
		}
	}

	deferredError = DeferErrorIfTargetHasFalseClause(targetRelationId,
													 plannerRestrictionContext);
	if (deferredError)
	{
		ereport(DEBUG1, (errmsg("Target relation has a filter of the "
								"form: false (AND ..), which results "
								"in empty shards, but we still need  "
								"to evaluate NOT-MATCHED clause, try "
								"repartitioning")));
		return deferredError;
	}


	/*
	 * If execution has reached this point, it indicates that the query can be delegated to the worker.
	 * However, before proceeding with this delegation, we need to confirm that the user is utilizing
	 * the distribution column of the source table in the Insert variable.
	 * If this is not the case, we should refrain from pushing down the query.
	 * This is just a deffered error which will be handle by caller.
	 */

	Var *insertVar =
		FetchAndValidateInsertVarIfExists(targetRelationId, query);
	if (insertVar &&
		!IsDistributionColumnInMergeSource((Expr *) insertVar, query, true))
	{
		ereport(DEBUG1, (errmsg(
							 "MERGE INSERT must use the source table distribution column value for push down to workers. Otherwise, repartitioning will be applied")));
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE INSERT must use the source table distribution column value for push down to workers. Otherwise, repartitioning will be applied",
							 NULL, NULL);
	}
	return NULL;
}


/*
 * MergeSourceHasRouterSelect is a helper function that returns true of the source
 * part of the merge query is a router query.
 */
static bool
MergeSourceHasRouterSelect(Query *query,
						   PlannerRestrictionContext *plannerRestrictionContext)
{
	Query *copiedQuery = copyObject(query);
	RangeTblEntry *mergeSourceRte = ExtractMergeSourceRangeTableEntry(copiedQuery, true);

	if (mergeSourceRte == NULL)
	{
		/*
		 * We might potentially support this case in the future, but for now,
		 * we don't support MERGE with JOIN in the source.
		 */
		return false;
	}

	ConvertSourceRTEIntoSubquery(copiedQuery, mergeSourceRte, plannerRestrictionContext);
	Query *sourceQuery = mergeSourceRte->subquery;

	DistributedPlan *distributedPlan = CreateRouterPlan(sourceQuery, sourceQuery,
														plannerRestrictionContext);

	return distributedPlan->planningError == NULL;
}


/*
 * ErrorIfMergeQueryQualAndTargetListNotSupported does check for a MERGE command in the query, if it finds
 * one, it will verify the below criteria
 * - Distributed tables co-location requirements
 * - Checks target-lists and functions-in-quals in TargetlistAndFunctionsSupported
 */
static void
ErrorIfMergeQueryQualAndTargetListNotSupported(Oid targetRelationId, Query *originalQuery)
{
	/*
	 * TODO: For now, we are adding an exception where any volatile or stable
	 * functions are not allowed in the MERGE query, but this will become too
	 * restrictive as this will prevent many useful and simple cases, such as,
	 * INSERT VALUES(ts::timestamp), bigserial column inserts etc. But without
	 * this restriction, we have a potential danger of some of the function(s)
	 * getting executed at the worker which will result in incorrect behavior.
	 */
	if (contain_mutable_functions((Node *) originalQuery))
	{
		ereport(ERROR, (errmsg("non-IMMUTABLE functions are not yet "
							   "supported in MERGE sql with distributed tables")));
	}

	Node *joinCondition = GetMergeJoinCondition(originalQuery);

	DeferredErrorMessage *deferredError =
		MergeQualAndTargetListFunctionsSupported(
			targetRelationId,
			originalQuery,
			joinCondition,
			originalQuery->targetList,
			originalQuery->commandType);

	if (deferredError)
	{
		RaiseDeferredError(deferredError, ERROR);
	}

	/*
	 * MERGE is a special case where we have multiple modify statements
	 * within itself. Check each INSERT/UPDATE/DELETE individually.
	 */
	MergeAction *action = NULL;
	foreach_declared_ptr(action, originalQuery->mergeActionList)
	{
		Assert(originalQuery->returningList == NULL);
		deferredError = MergeQualAndTargetListFunctionsSupported(targetRelationId,
																 originalQuery,
																 action->qual,
																 action->targetList,
																 action->commandType);
		if (deferredError)
		{
			/* MERGE's unsupported scenario, raise the exception */
			RaiseDeferredError(deferredError, ERROR);
		}
	}
}


/*
 * MergeCommandResultIdPrefix returns the prefix to use for intermediate results of
 * an MERGE INTO ... USING source-query results via the coordinator.
 */
static char *
MergeCommandResultIdPrefix(uint64 planId)
{
	StringInfo resultIdPrefix = makeStringInfo();
	appendStringInfo(resultIdPrefix, "merge_into_" UINT64_FORMAT, planId);
	return resultIdPrefix->data;
}


/*
 * ValidateAndReturnVarIfSupported Checks for valid expressions of type Var, and
 * returns the Var if it finds one, for everything else, raises an exception.
 */
static Var *
ValidateAndReturnVarIfSupported(Node *entryExpr)
{
	if (!IsA(entryExpr, Var))
	{
		ereport(ERROR, (errmsg("MERGE INSERT is using unsupported expression type "
							   "for distribution column"),
						errdetail("Inserting arbitrary values that don't correspond "
								  "to the joined column values can lead to unpredictable "
								  "outcomes where rows are incorrectly distributed "
								  "among different shards")));
	}

	/* Found a Var inserting into target's distribution column */
	return (Var *) entryExpr;
}


/*
 * SourceResultPartitionColumnIndex collects all Join conditions from the
 * ON clause and verifies if there is a join, either left or right, with
 * the distribution column of the given target. Once a match is found, it
 * returns the index of that match in the source's target list.
 */
static int
SourceResultPartitionColumnIndex(Query *mergeQuery, List *sourceTargetList,
								 CitusTableCacheEntry *targetRelation)
{
	List *mergeJoinConditionList = WhereClauseList(GetMergeJoinTree(mergeQuery));
	Var *targetColumn = targetRelation->partitionColumn;
	Var *sourceRepartitionVar = NULL;
	bool foundTypeMismatch = false;

	OpExpr *validJoinClause =
		SinglePartitionJoinClause(list_make1(targetColumn), mergeJoinConditionList,
								  &foundTypeMismatch);
	if (!validJoinClause)
	{
		if (foundTypeMismatch)
		{
			ereport(ERROR, (errmsg("In the MERGE ON clause, there is a datatype mismatch "
								   "between target's distribution "
								   "column and the expression originating from the source."),
							errdetail(
								"If the types are different, Citus uses different hash "
								"functions for the two column types, which might "
								"lead to incorrect repartitioning of the result data")));
		}

		ereport(ERROR, (errmsg("The required join operation is missing between "
							   "the target's distribution column and any "
							   "expression originating from the source. The "
							   "issue may arise from a non-equi-join."),
						errdetail("Without a equi-join condition on the target's "
								  "distribution column, the source rows "
								  "cannot be efficiently redistributed, and "
								  "the NOT-MATCHED condition cannot be evaluated "
								  "unambiguously. This can result in incorrect or "
								  "unexpected results when attempting to merge "
								  "tables in a distributed setting")));
	}

	/* both are verified in SinglePartitionJoinClause to not be NULL, assert is to guard */
	Var *leftColumn = LeftColumnOrNULL(validJoinClause);
	Var *rightColumn = RightColumnOrNULL(validJoinClause);

	Assert(leftColumn != NULL);
	Assert(rightColumn != NULL);

	if (equal(targetColumn, leftColumn))
	{
		sourceRepartitionVar = rightColumn;
	}
	else if (equal(targetColumn, rightColumn))
	{
		sourceRepartitionVar = leftColumn;
	}

	/* Either we find an insert-action or it's not relevant for certain class of tables */
	Var *insertVar =
		FetchAndValidateInsertVarIfExists(targetRelation->relationId, mergeQuery);
	if (insertVar)
	{
		/* INSERT action, must choose joining column for inserted value */
		bool joinedOnInsertColumn =
			JoinOnColumns(list_make1(targetColumn), insertVar, mergeJoinConditionList);
		if (joinedOnInsertColumn)
		{
			sourceRepartitionVar = insertVar;
		}
		else
		{
			ereport(ERROR, (errmsg("MERGE INSERT must use the "
								   "source's joining column for "
								   "target's distribution column")));
		}
	}

	Assert(sourceRepartitionVar);

	int sourceResultRepartitionColumnIndex =
		DistributionColumnIndex(sourceTargetList, sourceRepartitionVar);

	if (sourceResultRepartitionColumnIndex == -1)
	{
		ereport(ERROR,
				(errmsg("Unexpected column index of the source list")));
	}
	else
	{
		ereport(DEBUG1, (errmsg("Using column - index:%d from the source list "
								"to redistribute", sourceResultRepartitionColumnIndex)));
	}

	return sourceResultRepartitionColumnIndex;
}


/*
 * ExtractMergeSourceRangeTableEntry returns the range table entry of source
 * table or source query in USING clause.
 */
RangeTblEntry *
ExtractMergeSourceRangeTableEntry(Query *query, bool joinSourceOk)
{
	Assert(IsMergeQuery(query));

	List *fromList = query->jointree->fromlist;

	/*
	 * We should have only one RTE(MergeStmt->sourceRelation) in the from-list
	 * unless Postgres community changes the representation of merge.
	 */
	if (list_length(fromList) != 1)
	{
		ereport(ERROR, (errmsg("Unexpected source list in MERGE sql USING clause")));
	}

	RangeTblRef *reference = linitial(fromList);

	/*
	 * The planner sometimes generates JoinExprs internally; these can
	 * have rtindex = 0 if there are no join alias variables referencing
	 * such joins.
	 */
	if (reference->rtindex == 0)
	{
		if (!joinSourceOk)
		{
			ereport(ERROR, (errmsg("Source is not an explicit query"),
							errhint("Source query is a Join expression, "
									"try converting into a query as SELECT * "
									"FROM (..Join..)")));
		}

		return NULL;
	}


	Assert(reference->rtindex >= 1);
	RangeTblEntry *subqueryRte = rt_fetch(reference->rtindex, query->rtable);

	return subqueryRte;
}


/*
 * FetchAndValidateInsertVarIfExists checks to see if MERGE is inserting a
 * value into the target which is not from the source table, if so, it
 * raises an exception. The return value is the Var that's being inserted
 * into the target's distribution column, If no INSERT action exist, it
 * simply returns a NULL.
 * Note: Inserting random values other than the joined column values will
 * result in unexpected behaviour of rows ending up in incorrect shards, to
 * prevent such mishaps, we disallow such inserts here.
 */
Var *
FetchAndValidateInsertVarIfExists(Oid targetRelationId, Query *query)
{
	Assert(IsMergeQuery(query));

	if (!IsCitusTableType(targetRelationId, DISTRIBUTED_TABLE))
	{
		return NULL;
	}

	if (!HasDistributionKey(targetRelationId))
	{
		return NULL;
	}

	bool foundDistributionColumn = false;
	MergeAction *action = NULL;
	uint32 targetRangeTableIndex = query->resultRelation;
	foreach_declared_ptr(action, query->mergeActionList)
	{
		/* Skip MATCHED clause as INSERTS are not allowed in it */
		if (matched_compat(action))
		{
			continue;
		}

		/* NOT MATCHED can have either INSERT, DO NOTHING or UPDATE(PG17) */
		if (action->commandType == CMD_NOTHING || action->commandType == CMD_UPDATE)
		{
			return NULL;
		}

		if (action->targetList == NIL)
		{
			/* INSERT DEFAULT VALUES is not allowed */
			ereport(ERROR, (errmsg("cannot perform MERGE INSERT with DEFAULTS"),
							errdetail("Inserting arbitrary values that don't correspond "
									  "to the joined column values can lead to "
									  "unpredictable outcomes where rows are "
									  "incorrectly distributed among different "
									  "shards")));
		}

		Assert(action->commandType == CMD_INSERT);
		Var *targetDistributionKey =
			PartitionColumn(targetRelationId, targetRangeTableIndex);

		TargetEntry *targetEntry = NULL;
		foreach_declared_ptr(targetEntry, action->targetList)
		{
			AttrNumber originalAttrNo = targetEntry->resno;

			/* skip processing of target table non-distribution columns */
			if (originalAttrNo != targetDistributionKey->varattno)
			{
				continue;
			}

			foundDistributionColumn = true;

			Node *insertExpr =
				strip_implicit_coercions((Node *) copyObject(targetEntry->expr));
			return ValidateAndReturnVarIfSupported(insertExpr);
		}

		if (!foundDistributionColumn)
		{
			ereport(ERROR,
					(errmsg("MERGE INSERT must have distribution column as value")));
		}
	}

	return NULL;
}


/*
 * IsLocalTableModification returns true if the table modified is a Postgres table.
 * We do not support recursive planning for MERGE yet, so we could have a join
 * between local and Citus tables. Only allow local tables when it is the target table.
 */
bool
IsLocalTableModification(Oid targetRelationId, Query *query, uint64 shardId,
						 RTEListProperties *rteProperties)
{
	/* No-op for SELECT command */
	if (!IsModifyCommand(query))
	{
		return false;
	}

	/* For MERGE, we have to check only the target relation */
	if (IsMergeQuery(query) && !IsCitusTable(targetRelationId))
	{
		/* Postgres table */
		return true;
	}

	if (shardId == INVALID_SHARD_ID && ContainsOnlyLocalTables(rteProperties))
	{
		return true;
	}

	return false;
}
