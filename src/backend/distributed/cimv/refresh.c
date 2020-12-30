#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_cimv.h"
#include "executor/spi.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "distributed/cimv.h"

static void SpiExecuteSnapshot(StringInfo query, Snapshot snapshot, int expectedResult);
static void SpiExecute(StringInfo query, int expectedResult);

bool
ProcessRefreshMaterializedViewStmt(RefreshMatViewStmt *stmt)
{
	Oid relationId = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (relationId == InvalidOid)
	{
		return false;
	}

	Form_pg_cimv formCimv = LookupCimvFromCatalog(relationId, true);

	if (formCimv == NULL)
	{
		return false;
	}

	RefreshCimv(formCimv, stmt->skipData, false);
	return true;
}


void
RefreshCimv(Form_pg_cimv formCimv, bool skipData, bool isCreate)
{
	StringInfoData querybuf;
	initStringInfo(&querybuf);

	if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	/* make sure we have write access */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	const char *matTableSchemaName = get_namespace_name(get_rel_namespace(
															formCimv->mattable));
	const char *matTableName = get_rel_name(formCimv->mattable);
	matTableSchemaName = quote_identifier(matTableSchemaName);
	matTableName = quote_identifier(matTableName);

	const char *landingTableSchemaName = NULL;
	const char *landingTableName = NULL;
	if (formCimv->landingtable)
	{
		landingTableSchemaName = get_namespace_name(get_rel_namespace(
														formCimv->landingtable));
		landingTableName = get_rel_name(formCimv->landingtable);
		landingTableSchemaName = quote_identifier(landingTableSchemaName);
		landingTableName = quote_identifier(landingTableName);
	}

	if (skipData)
	{
		if (formCimv->landingtable)
		{
			appendStringInfo(&querybuf,
							 "TRUNCATE TABLE %s.%s",
							 landingTableSchemaName,
							 landingTableName);

			SpiExecute(&querybuf, SPI_OK_UTILITY);
			resetStringInfo(&querybuf);
		}
		appendStringInfo(&querybuf,
						 "TRUNCATE TABLE  %s.%s",
						 matTableSchemaName,
						 matTableName);

		SpiExecute(&querybuf, SPI_OK_UTILITY);
		resetStringInfo(&querybuf);
	}
	else
	{
		const char *refreshViewSchemaName = get_namespace_name(get_rel_namespace(
																   formCimv->refreshview));
		const char *refreshViewName = get_rel_name(formCimv->refreshview);
		refreshViewSchemaName = quote_identifier(refreshViewSchemaName);
		refreshViewName = quote_identifier(refreshViewName);

		if (isCreate)
		{
			/* better: SPI_commit_and_chain(); */
			SPI_commit();
			SPI_start_transaction();

			/* TODO: cleanup if this fails */
			appendStringInfo(&querybuf,
							 "INSERT INTO %s.%s "
							 "SELECT * FROM %s.%s",
							 matTableSchemaName,
							 matTableName,
							 refreshViewSchemaName,
							 refreshViewName);
			if (SPI_execute(querybuf.data, false, 0) != SPI_OK_INSERT)
			{
				elog(ERROR, "SPI_exec failed: %s", querybuf.data);
			}
		}
		else
		{
			Snapshot snapshot = GetLatestSnapshot();

			/* TODO: DELETE only if !isCreate */
			appendStringInfo(&querybuf,
							 "DELETE FROM  %s.%s",
							 matTableSchemaName,
							 matTableName);
			SpiExecuteSnapshot(&querybuf, snapshot, SPI_OK_DELETE);
			resetStringInfo(&querybuf);

			appendStringInfo(&querybuf,
							 "INSERT INTO %s.%s "
							 "SELECT * FROM %s.%s",
							 matTableSchemaName,
							 matTableName,
							 refreshViewSchemaName,
							 refreshViewName);
			SpiExecuteSnapshot(&querybuf, snapshot, SPI_OK_INSERT);
			resetStringInfo(&querybuf);

			if (formCimv->landingtable != InvalidOid)
			{
				/* TODO: DELETE only if !isCreate */
				appendStringInfo(&querybuf,
								 "DELETE FROM  %s.%s",
								 landingTableSchemaName,
								 landingTableName);
				SpiExecuteSnapshot(&querybuf, snapshot, SPI_OK_DELETE);
				resetStringInfo(&querybuf);
			}
		}
	}

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed");
	}
}


static void
SpiExecuteSnapshot(StringInfo query, Snapshot snapshot, int expectedResult)
{
	SPIPlanPtr qplan = SPI_prepare(query->data, 0, NULL);

	if (qplan == NULL)
	{
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), query->data);
	}

	int spi_result = SPI_execute_snapshot(qplan,
										  NULL, NULL,
										  snapshot,
										  InvalidSnapshot,
										  false, false, 0);

	if (spi_result != expectedResult)
	{
		elog(ERROR, "SPI_exec failed: %s", query->data);
	}
}


static void
SpiExecute(StringInfo query, int expectedResult)
{
	int spi_result = SPI_execute(query->data, false, 0);

	if (spi_result != expectedResult)
	{
		elog(ERROR, "SPI_exec failed: %s", query->data);
	}
}
