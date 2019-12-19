/*-------------------------------------------------------------------------
 *
 * shard_rebalancer.c
 *
 * Function definitions for the shard rebalancer tool.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "distributed/enterprise.h"
#include "utils/syscache.h"


static void EnsureShardCostUDF(Oid functionOid);
static void EnsureNodeCapacityUDF(Oid functionOid);
static void EnsureShardAllowedOnNodeUDF(Oid functionOid);

NOT_SUPPORTED_IN_COMMUNITY(rebalance_table_shards);
NOT_SUPPORTED_IN_COMMUNITY(replicate_table_shards);
NOT_SUPPORTED_IN_COMMUNITY(get_rebalance_table_shards_plan);
NOT_SUPPORTED_IN_COMMUNITY(get_rebalance_progress);
NOT_SUPPORTED_IN_COMMUNITY(master_drain_node);
NOT_SUPPORTED_IN_COMMUNITY(citus_shard_cost_by_disk_size);
PG_FUNCTION_INFO_V1(pg_dist_rebalance_strategy_enterprise_check);
PG_FUNCTION_INFO_V1(citus_validate_rebalance_strategy_functions);


/*
 * citus_rebalance_strategy_enterprise_check is trigger function, intended for
 * use in prohibiting writes to pg_dist_rebalance_strategy in Citus Community.
 */
Datum
pg_dist_rebalance_strategy_enterprise_check(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot write to pg_dist_rebalance_strategy"),
					errdetail(
						"Citus Community Edition does not support the use of "
						"custom rebalance strategies."),
					errhint(
						"To learn more about using advanced rebalancing schemes "
						"with Citus, please contact us at "
						"https://citusdata.com/about/contact_us")));
}


/*
 * citus_validate_rebalance_strategy_functions checks all the functions for
 * their correct signature.
 *
 * SQL signature:
 *
 * citus_validate_rebalance_strategy_functions(
 *     shard_cost_function regproc,
 *     node_capacity_function regproc,
 *     shard_allowed_on_node_function regproc,
 * ) RETURNS VOID
 */
Datum
citus_validate_rebalance_strategy_functions(PG_FUNCTION_ARGS)
{
	EnsureShardCostUDF(PG_GETARG_OID(0));
	EnsureNodeCapacityUDF(PG_GETARG_OID(1));
	EnsureShardAllowedOnNodeUDF(PG_GETARG_OID(2));
	PG_RETURN_VOID();
}


/*
 * EnsureShardCostUDF checks that the UDF matching the oid has the correct
 * signature to be used as a ShardCost function. The expected signature is:
 *
 * shard_cost(shardid bigint) returns float4
 */
static void
EnsureShardCostUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg("cache lookup failed for shard_cost_function with oid %u",
							   functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 1)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 1, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT8OID)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail(
							"argument type of %s should be bigint", name)));
	}
	if (procForm->prorettype != FLOAT4OID)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail("return type of %s should be real", name)));
	}
	ReleaseSysCache(proctup);
}


/*
 * EnsureNodeCapacityUDF checks that the UDF matching the oid has the correct
 * signature to be used as a NodeCapacity function. The expected signature is:
 *
 * node_capacity(nodeid int) returns float4
 */
static void
EnsureNodeCapacityUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg(
							"cache lookup failed for node_capacity_function with oid %u",
							functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 1)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 1, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT4OID)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail("argument type of %s should be int", name)));
	}
	if (procForm->prorettype != FLOAT4OID)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail("return type of %s should be real", name)));
	}
	ReleaseSysCache(proctup);
}


/*
 * EnsureNodeCapacityUDF checks that the UDF matching the oid has the correct
 * signature to be used as a NodeCapacity function. The expected signature is:
 *
 * shard_allowed_on_node(shardid bigint, nodeid int) returns boolean
 */
static void
EnsureShardAllowedOnNodeUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg(
							"cache lookup failed for shard_allowed_on_node_function with oid %u",
							functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 2)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 2, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT8OID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"type of first argument of %s should be bigint", name)));
	}
	if (procForm->proargtypes.values[1] != INT4OID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"type of second argument of %s should be int", name)));
	}
	if (procForm->prorettype != BOOLOID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"return type of %s should be boolean", name)));
	}
	ReleaseSysCache(proctup);
}
