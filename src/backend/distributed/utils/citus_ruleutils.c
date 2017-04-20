/*-------------------------------------------------------------------------
 *
 * citus_ruleutils.c
 *	  Version independent ruleutils wrapper
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"

#include <stddef.h>

#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_index.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relay_utility.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_utilcmd.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


static void AppendOptionListToString(StringInfo stringData, List *options);
static const char * convert_aclright_to_string(int aclright);
static bool contain_nextval_expression_walker(Node *node, void *context);

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
Oid
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
 * pg_get_sequencedef_string returns the definition of a given sequence. This
 * definition includes explicit values for all CREATE SEQUENCE options.
 */
char *
pg_get_sequencedef_string(Oid sequenceRelationId)
{
	char *qualifiedSequenceName = NULL;
	char *sequenceDef = NULL;
	Form_pg_sequence pgSequenceForm = NULL;

	pgSequenceForm = pg_get_sequencedef(sequenceRelationId);

	/* build our DDL command */
	qualifiedSequenceName = generate_relation_name(sequenceRelationId, NIL);
	sequenceDef = psprintf(CREATE_SEQUENCE_COMMAND, qualifiedSequenceName,
						   pgSequenceForm->increment_by, pgSequenceForm->min_value,
						   pgSequenceForm->max_value, pgSequenceForm->start_value,
						   pgSequenceForm->is_cycled ? "" : "NO ");

	return sequenceDef;
}


/*
 * pg_get_sequencedef returns the Form_pg_sequence data about the sequence with the given
 * object id.
 */
Form_pg_sequence
pg_get_sequencedef(Oid sequenceRelationId)
{
	Form_pg_sequence pgSequenceForm = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Relation sequenceRel = NULL;
	AclResult permissionCheck = ACLCHECK_NO_PRIV;

	/* open and lock sequence */
	sequenceRel = heap_open(sequenceRelationId, AccessShareLock);

	/* check permissions to read sequence attributes */
	permissionCheck = pg_class_aclcheck(sequenceRelationId, GetUserId(),
										ACL_SELECT | ACL_USAGE);
	if (permissionCheck != ACLCHECK_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied for sequence %s",
							   RelationGetRelationName(sequenceRel))));
	}

	/* retrieve attributes from first tuple */
	scanDescriptor = systable_beginscan(sequenceRel, InvalidOid, false, NULL, 0, NULL);
	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find specified sequence")));
	}

	pgSequenceForm = (Form_pg_sequence) GETSTRUCT(heapTuple);

	systable_endscan(scanDescriptor);

	heap_close(sequenceRel, AccessShareLock);

	return pgSequenceForm;
}


/*
 * pg_get_tableschemadef_string returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.
 * When the flag includeSequenceDefaults is set, the function also creates
 * DEFAULT clauses for columns getting their default values from a sequence.
 */
char *
pg_get_tableschemadef_string(Oid tableRelationId, bool includeSequenceDefaults)
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

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			const char *attributeName = NULL;
			const char *attributeTypeName = NULL;

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

				/*
				 * if column default value is explicitly requested, or it is
				 * not set from a sequence then we include DEFAULT clause for
				 * this column.
				 */
				if (includeSequenceDefaults ||
					!contain_nextval_expression_walker(defaultNode, NULL))
				{
					defaultContext = deparse_context_for(relationName, tableRelationId);

					/* deparse default value string */
					defaultString = deparse_expression(defaultNode, defaultContext,
													   false, false);

					appendStringInfo(&buffer, " DEFAULT %s", defaultString);
				}
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
		char *columnOptionStatement = NULL;

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
 * deparse_shard_index_statement uses the provided CREATE INDEX node, dist.
 * relation, and shard identifier to populate a provided buffer with a string
 * representation of a shard-extended version of that command.
 */
void
deparse_shard_index_statement(IndexStmt *origStmt, Oid distrelid, int64 shardid,
							  StringInfo buffer)
{
	IndexStmt *indexStmt = copyObject(origStmt); /* copy to avoid modifications */
	char *relationName = indexStmt->relation->relname;
	char *indexName = indexStmt->idxname;
	ListCell *indexParameterCell = NULL;
	List *deparseContext = NULL;

	/* extend relation and index name using shard identifier */
	AppendShardIdToName(&relationName, shardid);
	AppendShardIdToName(&indexName, shardid);

	/* use extended shard name and transformed stmt for deparsing */
	deparseContext = deparse_context_for(relationName, distrelid);
	indexStmt = transformIndexStmt(distrelid, indexStmt, NULL);

	appendStringInfo(buffer, "CREATE %s INDEX %s %s %s ON %s USING %s ",
					 (indexStmt->unique ? "UNIQUE" : ""),
					 (indexStmt->concurrent ? "CONCURRENTLY" : ""),
					 (indexStmt->if_not_exists ? "IF NOT EXISTS" : ""),
					 quote_identifier(indexName),
					 quote_qualified_identifier(indexStmt->relation->schemaname,
												relationName),
					 indexStmt->accessMethod);

	/* index column or expression list begins here */
	appendStringInfoChar(buffer, '(');

	foreach(indexParameterCell, indexStmt->indexParams)
	{
		IndexElem *indexElement = (IndexElem *) lfirst(indexParameterCell);

		/* use commas to separate subsequent elements */
		if (indexParameterCell != list_head(indexStmt->indexParams))
		{
			appendStringInfoChar(buffer, ',');
		}

		if (indexElement->name)
		{
			appendStringInfo(buffer, "%s ", quote_identifier(indexElement->name));
		}
		else if (indexElement->expr)
		{
			appendStringInfo(buffer, "(%s)", deparse_expression(indexElement->expr,
																deparseContext, false,
																false));
		}

		if (indexElement->collation != NIL)
		{
			appendStringInfo(buffer, "COLLATE %s ",
							 NameListToQuotedString(indexElement->collation));
		}

		if (indexElement->opclass != NIL)
		{
			appendStringInfo(buffer, "%s ",
							 NameListToQuotedString(indexElement->opclass));
		}

		if (indexElement->ordering != SORTBY_DEFAULT)
		{
			bool sortAsc = (indexElement->ordering == SORTBY_ASC);
			appendStringInfo(buffer, "%s ", (sortAsc ? "ASC" : "DESC"));
		}

		if (indexElement->nulls_ordering != SORTBY_NULLS_DEFAULT)
		{
			bool nullsFirst = (indexElement->nulls_ordering == SORTBY_NULLS_FIRST);
			appendStringInfo(buffer, "NULLS %s ", (nullsFirst ? "FIRST" : "LAST"));
		}
	}

	appendStringInfoString(buffer, ") ");

	if (indexStmt->options != NIL)
	{
		appendStringInfoString(buffer, "WITH ");
		AppendOptionListToString(buffer, indexStmt->options);
	}

	if (indexStmt->whereClause != NULL)
	{
		appendStringInfo(buffer, "WHERE %s", deparse_expression(indexStmt->whereClause,
																deparseContext, false,
																false));
	}
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


/*
 * pg_get_table_grants returns a list of sql statements which recreate the
 * permissions for a specific table.
 *
 * This function is modeled after aclexplode(), don't change too heavily.
 */
List *
pg_get_table_grants(Oid relationId)
{
	/* *INDENT-OFF* */
	StringInfoData buffer;
	Relation relation = NULL;
	char *relationName = NULL;
	List *defs = NIL;
	HeapTuple classTuple = NULL;
	Datum aclDatum = 0;
	bool isNull = false;

	relation = relation_open(relationId, AccessShareLock);
	relationName = generate_relation_name(relationId, NIL);

	initStringInfo(&buffer);

	/* lookup all table level grants */
	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(classTuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist",
						relationId)));
	}

	aclDatum = SysCacheGetAttr(RELOID, classTuple, Anum_pg_class_relacl,
							   &isNull);

	ReleaseSysCache(classTuple);

	if (!isNull)
	{
		int i = 0;
		AclItem *aidat = NULL;
		Acl *acl = NULL;
		int offtype = 0;

		/*
		 * First revoke all default permissions, so we can start adding the
		 * exact permissions from the master. Note that we only do so if there
		 * are any actual grants; an empty grant set signals default
		 * permissions.
		 *
		 * Note: This doesn't work correctly if default permissions have been
		 * changed with ALTER DEFAULT PRIVILEGES - but that's hard to fix
		 * properly currently.
		 */
		appendStringInfo(&buffer, "REVOKE ALL ON %s FROM PUBLIC",
						 relationName);
		defs = lappend(defs, pstrdup(buffer.data));
		resetStringInfo(&buffer);

		/* iterate through the acl datastructure, emit GRANTs */

		acl = DatumGetAclP(aclDatum);
		aidat = ACL_DAT(acl);

		offtype = -1;
		i = 0;
		while (i < ACL_NUM(acl))
		{
			AclItem    *aidata = NULL;
			AclMode		priv_bit = 0;

			offtype++;

			if (offtype == N_ACL_RIGHTS)
			{
				offtype = 0;
				i++;
				if (i >= ACL_NUM(acl)) /* done */
				{
					break;
				}
			}

			aidata = &aidat[i];
			priv_bit = 1 << offtype;

			if (ACLITEM_GET_PRIVS(*aidata) & priv_bit)
			{
				const char *roleName = NULL;
				const char *withGrant = "";

				if (aidata->ai_grantee != 0)
				{
					HeapTuple htup;

					htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aidata->ai_grantee));
					if (HeapTupleIsValid(htup))
					{
						Form_pg_authid authForm = ((Form_pg_authid) GETSTRUCT(htup));

						roleName = quote_identifier(NameStr(authForm->rolname));

						ReleaseSysCache(htup);
					}
					else
					{
						elog(ERROR, "cache lookup failed for role %u", aidata->ai_grantee);
					}
				}
				else
				{
					roleName = "PUBLIC";
				}

				if ((ACLITEM_GET_GOPTIONS(*aidata) & priv_bit) != 0)
				{
					withGrant = " WITH GRANT OPTION";
				}

				appendStringInfo(&buffer, "GRANT %s ON %s TO %s%s",
								 convert_aclright_to_string(priv_bit),
								 relationName,
								 roleName,
								 withGrant);

				defs = lappend(defs, pstrdup(buffer.data));

				resetStringInfo(&buffer);
			}
		}
	}

	resetStringInfo(&buffer);

	relation_close(relation, NoLock);
	return defs;
	/* *INDENT-ON* */
}


/*
 * generate_qualified_relation_name computes the schema-qualified name to display for a
 * relation specified by OID.
 */
char *
generate_qualified_relation_name(Oid relid)
{
	HeapTuple tp;
	Form_pg_class reltup;
	char *relname;
	char *nspname;
	char *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
	{
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);
	if (!nspname)
	{
		elog(ERROR, "cache lookup failed for namespace %u",
			 reltup->relnamespace);
	}

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
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


/* copy of postgresql's function, which is static as well */
static const char *
convert_aclright_to_string(int aclright)
{
	/* *INDENT-OFF* */
	switch (aclright)
	{
		case ACL_INSERT:
			return "INSERT";
		case ACL_SELECT:
			return "SELECT";
		case ACL_UPDATE:
			return "UPDATE";
		case ACL_DELETE:
			return "DELETE";
		case ACL_TRUNCATE:
			return "TRUNCATE";
		case ACL_REFERENCES:
			return "REFERENCES";
		case ACL_TRIGGER:
			return "TRIGGER";
		case ACL_EXECUTE:
			return "EXECUTE";
		case ACL_USAGE:
			return "USAGE";
		case ACL_CREATE:
			return "CREATE";
		case ACL_CREATE_TEMP:
			return "TEMPORARY";
		case ACL_CONNECT:
			return "CONNECT";
		default:
			elog(ERROR, "unrecognized aclright: %d", aclright);
			return NULL;
	}
	/* *INDENT-ON* */
}


/*
 * contain_nextval_expression_walker walks over expression tree and returns
 * true if it contains call to 'nextval' function.
 */
static bool
contain_nextval_expression_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == F_NEXTVAL_OID)
		{
			return true;
		}
	}
	return expression_tree_walker(node, contain_nextval_expression_walker, context);
}
