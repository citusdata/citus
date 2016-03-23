/*-------------------------------------------------------------------------
 *
 * citus_ruleutils.c
 *	  Version independent ruleutils wrapper
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_ruleutils.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "parser/keywords.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#if (PG_VERSION_NUM >= 90500)
#include "utils/ruleutils.h"
#endif
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/xml.h"

static Oid get_extension_schema(Oid ext_oid);
static void AppendOptionListToString(StringInfo stringData, List *options);

/*
 * pg_get_extensiondef_string finds the foreign data wrapper that corresponds to
 * the given foreign tableId, and checks if an extension owns this foreign data
 * wrapper. If it does, the function returns the extension's definition. If not,
 * the function returns null.
 */
char *
pg_get_extensiondef_string(Oid tableRelationId)
{
	ForeignTable *foreignTable = GetForeignTable(tableRelationId);
	ForeignServer *server = GetForeignServer(foreignTable->serverid);
	ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);
	StringInfoData buffer = { NULL, 0, 0, 0 };

	Oid classId = ForeignDataWrapperRelationId;
	Oid objectId = server->fdwid;

	Oid extensionId = getExtensionOfObject(classId, objectId);
	if (OidIsValid(extensionId))
	{
		char *extensionName = get_extension_name(extensionId);
		Oid extensionSchemaId = get_extension_schema(extensionId);
		char *extensionSchema = get_namespace_name(extensionSchemaId);

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s",
						 quote_identifier(extensionName),
						 quote_identifier(extensionSchema));
	}
	else
	{
		ereport(NOTICE, (errmsg("foreign-data wrapper \"%s\" does not have an "
								"extension defined", foreignDataWrapper->fdwname)));
	}

	return (buffer.data);
}


/*
 * get_extension_schema - given an extension OID, fetch its extnamespace
 *
 * Returns InvalidOid if no such extension.
 */
static Oid
get_extension_schema(Oid ext_oid)
{
	/* *INDENT-OFF* */
	Oid			result;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
	/* *INDENT-ON* */
}


/*
 * pg_get_serverdef_string finds the foreign server that corresponds to the
 * given foreign tableId, and returns this server's definition.
 */
char *
pg_get_serverdef_string(Oid tableRelationId)
{
	ForeignTable *foreignTable = GetForeignTable(tableRelationId);
	ForeignServer *server = GetForeignServer(foreignTable->serverid);
	ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);

	StringInfoData buffer = { NULL, 0, 0, 0 };
	initStringInfo(&buffer);

	appendStringInfo(&buffer, "CREATE SERVER %s", quote_identifier(server->servername));
	if (server->servertype != NULL)
	{
		appendStringInfo(&buffer, " TYPE %s",
						 quote_literal_cstr(server->servertype));
	}
	if (server->serverversion != NULL)
	{
		appendStringInfo(&buffer, " VERSION %s",
						 quote_literal_cstr(server->serverversion));
	}

	appendStringInfo(&buffer, " FOREIGN DATA WRAPPER %s",
					 quote_identifier(foreignDataWrapper->fdwname));

	/* append server options, if any */
	AppendOptionListToString(&buffer, server->options);

	return (buffer.data);
}


/*
 * AppendOptionListToString converts the option list to its textual format, and
 * appends this text to the given string buffer.
 */
static void
AppendOptionListToString(StringInfo stringBuffer, List *optionList)
{
	if (optionList != NIL)
	{
		ListCell *optionCell = NULL;
		bool firstOptionPrinted = false;

		appendStringInfo(stringBuffer, " OPTIONS (");

		foreach(optionCell, optionList)
		{
			DefElem *option = (DefElem *) lfirst(optionCell);
			char *optionName = option->defname;
			char *optionValue = defGetString(option);

			if (firstOptionPrinted)
			{
				appendStringInfo(stringBuffer, ", ");
			}
			firstOptionPrinted = true;

			appendStringInfo(stringBuffer, "%s ", quote_identifier(optionName));
			appendStringInfo(stringBuffer, "%s", quote_literal_cstr(optionValue));
		}

		appendStringInfo(stringBuffer, ")");
	}
}


/*
 * pg_get_tableschemadef_string returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.
 */
char *
pg_get_tableschemadef_string(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	TupleConstr *tupleConstraints = NULL;
	int attributeIndex = 0;
	bool firstAttributePrinted = false;
	AttrNumber defaultValueIndex = 0;
	AttrNumber constraintIndex = 0;
	AttrNumber constraintCount = 0;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs as other functions in
	 * ruleutils.c do, we follow an unusual approach here: we open the relation,
	 * and fetch the relation's tuple descriptor. We do this because the tuple
	 * descriptor already contains information harnessed from pg_attrdef,
	 * pg_attribute, pg_constraint, and pg_class; and therefore using the
	 * descriptor saves us from a lot of additional work.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId, NIL);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	initStringInfo(&buffer);
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(&buffer, "CREATE TABLE %s (", relationName);
	}
	else
	{
		appendStringInfo(&buffer, "CREATE FOREIGN TABLE %s (", relationName);
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and its
	 * formatted type.
	 */
	tupleDescriptor = RelationGetDescr(relation);
	tupleConstraints = tupleDescriptor->constr;

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];
		const char *attributeName = NULL;
		const char *attributeTypeName = NULL;

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			if (firstAttributePrinted)
			{
				appendStringInfoString(&buffer, ", ");
			}
			firstAttributePrinted = true;

			attributeName = NameStr(attributeForm->attname);
			appendStringInfo(&buffer, "%s ", quote_identifier(attributeName));

			attributeTypeName = format_type_with_typemod(attributeForm->atttypid,
														 attributeForm->atttypmod);
			appendStringInfoString(&buffer, attributeTypeName);

			/* if this column has a default value, append the default value */
			if (attributeForm->atthasdef)
			{
				AttrDefault *defaultValueList = NULL;
				AttrDefault *defaultValue = NULL;

				Node *defaultNode = NULL;
				List *defaultContext = NULL;
				char *defaultString = NULL;

				Assert(tupleConstraints != NULL);

				defaultValueList = tupleConstraints->defval;
				Assert(defaultValueList != NULL);

				defaultValue = &(defaultValueList[defaultValueIndex]);
				defaultValueIndex++;

				Assert(defaultValue->adnum == (attributeIndex + 1));
				Assert(defaultValueIndex <= tupleConstraints->num_defval);

				/* convert expression to node tree, and prepare deparse context */
				defaultNode = (Node *) stringToNode(defaultValue->adbin);
				defaultContext = deparse_context_for(relationName, tableRelationId);

				/* deparse default value string */
				defaultString = deparse_expression(defaultNode, defaultContext,
												   false, false);

				appendStringInfo(&buffer, " DEFAULT %s", defaultString);
			}

			/* if this column has a not null constraint, append the constraint */
			if (attributeForm->attnotnull)
			{
				appendStringInfoString(&buffer, " NOT NULL");
			}
		}
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number of
	 * check constraints here. Then iterate over all check constraints and print
	 * them.
	 */
	if (tupleConstraints != NULL)
	{
		constraintCount = tupleConstraints->num_check;
	}

	for (constraintIndex = 0; constraintIndex < constraintCount; constraintIndex++)
	{
		ConstrCheck *checkConstraintList = tupleConstraints->check;
		ConstrCheck *checkConstraint = &(checkConstraintList[constraintIndex]);

		Node *checkNode = NULL;
		List *checkContext = NULL;
		char *checkString = NULL;

		/* if an attribute or constraint has been printed, format properly */
		if (firstAttributePrinted || constraintIndex > 0)
		{
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
						 quote_identifier(checkConstraint->ccname));

		/* convert expression to node tree, and prepare deparse context */
		checkNode = (Node *) stringToNode(checkConstraint->ccbin);
		checkContext = deparse_context_for(relationName, tableRelationId);

		/* deparse check constraint string */
		checkString = deparse_expression(checkNode, checkContext, false, false);

		appendStringInfoString(&buffer, checkString);
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(tableRelationId);
		ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

		char *serverName = foreignServer->servername;
		appendStringInfo(&buffer, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(&buffer, foreignTable->options);
	}

	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * pg_get_tablecolumnoptionsdef_string returns column storage type and column
 * statistics definitions for given table, _if_ these definitions differ from
 * their default values. The function returns null if all columns use default
 * values for their storage types and statistics.
 */
char *
pg_get_tablecolumnoptionsdef_string(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	AttrNumber attributeIndex = 0;
	char *columnOptionStatement = NULL;
	List *columnOptionList = NIL;
	ListCell *columnOptionCell = NULL;
	bool firstOptionPrinted = false;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs, we open the relation,
	 * and use the relation's tuple descriptor to access attribute information.
	 * This is primarily to maintain symmetry with pg_get_tableschemadef.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId, NIL);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, check if column storage or
	 * statistics statements need to be printed.
	 */
	tupleDescriptor = RelationGetDescr(relation);

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];
		char *attributeName = NameStr(attributeForm->attname);
		char defaultStorageType = get_typstorage(attributeForm->atttypid);

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			/*
			 * If the user changed the column's default storage type, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstorage != defaultStorageType)
			{
				char *storageName = 0;
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				switch (attributeForm->attstorage)
				{
					case 'p':
					{
						storageName = "PLAIN";
						break;
					}

					case 'e':
					{
						storageName = "EXTERNAL";
						break;
					}

					case 'm':
					{
						storageName = "MAIN";
						break;
					}

					case 'x':
					{
						storageName = "EXTENDED";
						break;
					}

					default:
					{
						ereport(ERROR, (errmsg("unrecognized storage type: %c",
											   attributeForm->attstorage)));
						break;
					}
				}

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STORAGE %s", storageName);

				columnOptionList = lappend(columnOptionList, statement.data);
			}

			/*
			 * If the user changed the column's statistics target, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstattarget >= 0)
			{
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STATISTICS %d",
								 attributeForm->attstattarget);

				columnOptionList = lappend(columnOptionList, statement.data);
			}
		}
	}

	/*
	 * Iterate over column storage and statistics statements that we created,
	 * and append them to a single alter table statement.
	 */
	foreach(columnOptionCell, columnOptionList)
	{
		if (!firstOptionPrinted)
		{
			initStringInfo(&buffer);
			appendStringInfo(&buffer, "ALTER TABLE ONLY %s ",
							 generate_relation_name(tableRelationId, NIL));
		}
		else
		{
			appendStringInfoString(&buffer, ", ");
		}
		firstOptionPrinted = true;

		columnOptionStatement = (char *) lfirst(columnOptionCell);
		appendStringInfoString(&buffer, columnOptionStatement);

		pfree(columnOptionStatement);
	}

	list_free(columnOptionList);
	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * pg_get_indexclusterdef_string returns the definition of a cluster statement
 * for given index. The function returns null if the table is not clustered on
 * given index.
 */
char *
pg_get_indexclusterdef_string(Oid indexRelationId)
{
	HeapTuple indexTuple = NULL;
	Form_pg_index indexForm = NULL;
	Oid tableRelationId = InvalidOid;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	indexTuple = SearchSysCache(INDEXRELID, ObjectIdGetDatum(indexRelationId), 0, 0, 0);
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for index %u", indexRelationId)));
	}

	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	tableRelationId = indexForm->indrelid;

	/* check if the table is clustered on this index */
	if (indexForm->indisclustered)
	{
		char *tableName = generate_relation_name(tableRelationId, NIL);
		char *indexName = get_rel_name(indexRelationId); /* needs to be quoted */

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "ALTER TABLE %s CLUSTER ON %s",
						 tableName, quote_identifier(indexName));
	}

	ReleaseSysCache(indexTuple);

	return (buffer.data);
}
