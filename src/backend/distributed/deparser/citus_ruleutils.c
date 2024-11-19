/*-------------------------------------------------------------------------
 *
 * citus_ruleutils.c
 *	  Version independent ruleutils wrapper
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include <stddef.h>

#include "postgres.h"

#include "miscadmin.h"

#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/sysattr.h"
#include "access/toast_compression.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
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

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/relay_utility.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"


static void deparse_index_columns(StringInfo buffer, List *indexParameterList,
								  List *deparseContext);
static void AppendStorageParametersToString(StringInfo stringBuffer,
											List *optionList);
static const char * convert_aclright_to_string(int aclright);
static void simple_quote_literal(StringInfo buf, const char *val);
static void AddVacuumParams(ReindexStmt *reindexStmt, StringInfo buffer);


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
 * get_extension_version - given an extension OID, fetch its extversion
 * or NULL if not found.
 */
char *
get_extension_version(Oid extensionId)
{
	char *versionName = NULL;

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyData entry[1];
	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(extensionId));

	SysScanDesc scanDesc = systable_beginscan(relation, ExtensionOidIndexId, true,
											  NULL, 1, entry);

	HeapTuple tuple = systable_getnext(scanDesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		bool isNull = false;
		Datum versionDatum = heap_getattr(tuple, Anum_pg_extension_extversion,
										  RelationGetDescr(relation), &isNull);
		if (!isNull)
		{
			versionName = text_to_cstring(DatumGetTextPP(versionDatum));
		}
	}

	systable_endscan(scanDesc);

	table_close(relation, AccessShareLock);

	return versionName;
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
	HeapTuple	tuple;
	ScanKeyData entry[1];

	rel = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	SysScanDesc scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	table_close(rel, AccessShareLock);

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

	appendStringInfo(&buffer, "CREATE SERVER IF NOT EXISTS %s",
					 quote_identifier(server->servername));
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
	Form_pg_sequence pgSequenceForm = pg_get_sequencedef(sequenceRelationId);

	/* build our DDL command */
	char *qualifiedSequenceName = generate_qualified_relation_name(sequenceRelationId);
	char *typeName = format_type_be(pgSequenceForm->seqtypid);

	char *sequenceDef = psprintf(CREATE_SEQUENCE_COMMAND,
								 get_rel_persistence(sequenceRelationId) ==
								 RELPERSISTENCE_UNLOGGED ? "UNLOGGED " : "",
								 qualifiedSequenceName,
								 typeName,
								 pgSequenceForm->seqincrement, pgSequenceForm->seqmin,
								 pgSequenceForm->seqmax, pgSequenceForm->seqstart,
								 pgSequenceForm->seqcache,
								 pgSequenceForm->seqcycle ? "" : "NO ");

	return sequenceDef;
}


/*
 * pg_get_sequencedef returns the Form_pg_sequence data about the sequence with the given
 * object id.
 */
Form_pg_sequence
pg_get_sequencedef(Oid sequenceRelationId)
{
	HeapTuple heapTuple = SearchSysCache1(SEQRELID, sequenceRelationId);
	if (!HeapTupleIsValid(heapTuple))
	{
		elog(ERROR, "cache lookup failed for sequence %u", sequenceRelationId);
	}

	Form_pg_sequence pgSequenceForm = (Form_pg_sequence) GETSTRUCT(heapTuple);

	ReleaseSysCache(heapTuple);

	return pgSequenceForm;
}


/*
 * pg_get_tableschemadef_string returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.
 * When includeSequenceDefaults is NEXTVAL_SEQUENCE_DEFAULTS, the function also creates
 * DEFAULT clauses for columns getting their default values from a sequence.
 * When it's WORKER_NEXTVAL_SEQUENCE_DEFAULTS, the function creates the DEFAULT
 * clause using worker_nextval('sequence') and not nextval('sequence')
 * When IncludeIdentities is NO_IDENTITY, the function does not include identity column
 * specifications. When it's INCLUDE_IDENTITY it creates GENERATED .. AS IDENTIY clauses.
 */
char *
pg_get_tableschemadef_string(Oid tableRelationId, IncludeSequenceDefaults
							 includeSequenceDefaults, IncludeIdentities
							 includeIdentityDefaults, char *accessMethod)
{
	bool firstAttributePrinted = false;
	AttrNumber defaultValueIndex = 0;
	AttrNumber constraintIndex = 0;
	AttrNumber constraintCount = 0;
	bool relIsPartition = false;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs as other functions in
	 * ruleutils.c do, we follow an unusual approach here: we open the relation,
	 * and fetch the relation's tuple descriptor. We do this because the tuple
	 * descriptor already contains information harnessed from pg_attrdef,
	 * pg_attribute, pg_constraint, and pg_class; and therefore using the
	 * descriptor saves us from a lot of additional work.
	 */
	Relation relation = relation_open(tableRelationId, AccessShareLock);
	char *relationName = generate_relation_name(tableRelationId, NIL);

	EnsureRelationKindSupported(tableRelationId);

	initStringInfo(&buffer);

	if (RegularTable(tableRelationId))
	{
		appendStringInfoString(&buffer, "CREATE ");

		if (relation->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		{
			appendStringInfoString(&buffer, "UNLOGGED ");
		}

		appendStringInfo(&buffer, "TABLE %s (", relationName);

		relIsPartition = relation->rd_rel->relispartition;
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
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	TupleConstr *tupleConstraints = tupleDescriptor->constr;

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		/*
		 * We disregard the inherited attributes (i.e., attinhcount > 0) here. The
		 * reasoning behind this is that Citus implements declarative partitioning
		 * by creating the partitions first and then sending
		 * "ALTER TABLE parent_table ATTACH PARTITION .." command. This may not play
		 * well with regular inherited tables, which isn't a big concern from Citus'
		 * perspective.
		 */
		if (!attributeForm->attisdropped)
		{
			if (firstAttributePrinted)
			{
				appendStringInfoString(&buffer, ", ");
			}
			firstAttributePrinted = true;

			const char *attributeName = NameStr(attributeForm->attname);
			appendStringInfo(&buffer, "%s ", quote_identifier(attributeName));

			const char *attributeTypeName = format_type_with_typemod(
				attributeForm->atttypid,
				attributeForm->
				atttypmod);
			appendStringInfoString(&buffer, attributeTypeName);

			if (CompressionMethodIsValid(attributeForm->attcompression))
			{
				appendStringInfo(&buffer, " COMPRESSION %s",
								 GetCompressionMethodName(attributeForm->attcompression));
			}

			/*
			 * If this is an identity column include its identity definition in the
			 * DDL only if its relation is not a partition. If it is a partition, any
			 * identity is inherited from the parent table by ATTACH PARTITION. This
			 * is Postgres 17+ behavior (commit 699586315); prior PG versions did not
			 * support identity columns in partitioned tables.
			 */
			if (attributeForm->attidentity && includeIdentityDefaults && !relIsPartition)
			{
				bool missing_ok = false;
				Oid seqOid = getIdentitySequence(identitySequenceRelation_compat(
													 relation),
												 attributeForm->attnum, missing_ok);

				if (includeIdentityDefaults == INCLUDE_IDENTITY)
				{
					Form_pg_sequence pgSequenceForm = pg_get_sequencedef(seqOid);
					char *sequenceDef = psprintf(
						" GENERATED %s AS IDENTITY (INCREMENT BY " INT64_FORMAT \
						" MINVALUE " INT64_FORMAT " MAXVALUE "
						INT64_FORMAT \
						" START WITH " INT64_FORMAT " CACHE "
						INT64_FORMAT " %sCYCLE)",
						attributeForm->attidentity == ATTRIBUTE_IDENTITY_ALWAYS ?
						"ALWAYS" : "BY DEFAULT",
						pgSequenceForm->seqincrement,
						pgSequenceForm->seqmin,
						pgSequenceForm->seqmax,
						pgSequenceForm->seqstart,
						pgSequenceForm->seqcache,
						pgSequenceForm->seqcycle ? "" : "NO ");

					appendStringInfo(&buffer, "%s", sequenceDef);
				}
			}

			/* if this column has a default value, append the default value */
			if (attributeForm->atthasdef)
			{
				List *defaultContext = NULL;
				char *defaultString = NULL;

				Assert(tupleConstraints != NULL);

				AttrDefault *defaultValueList = tupleConstraints->defval;
				Assert(defaultValueList != NULL);

				AttrDefault *defaultValue = &(defaultValueList[defaultValueIndex]);
				defaultValueIndex++;

				Assert(defaultValue->adnum == (attributeIndex + 1));
				Assert(defaultValueIndex <= tupleConstraints->num_defval);

				/* convert expression to node tree, and prepare deparse context */
				Node *defaultNode = (Node *) stringToNode(defaultValue->adbin);

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

					if (attributeForm->attgenerated == ATTRIBUTE_GENERATED_STORED)
					{
						appendStringInfo(&buffer, " GENERATED ALWAYS AS (%s) STORED",
										 defaultString);
					}
					else
					{
						Oid seqOid = GetSequenceOid(tableRelationId, defaultValue->adnum);
						if (includeSequenceDefaults == WORKER_NEXTVAL_SEQUENCE_DEFAULTS &&
							seqOid != InvalidOid &&
							pg_get_sequencedef(seqOid)->seqtypid != INT8OID)
						{
							/*
							 * We use worker_nextval for int and smallint types.
							 * Check issue #5126 and PR #5254 for details.
							 * https://github.com/citusdata/citus/issues/5126
							 */
							char *sequenceName = generate_qualified_relation_name(
								seqOid);
							appendStringInfo(&buffer,
											 " DEFAULT worker_nextval(%s::regclass)",
											 quote_literal_cstr(sequenceName));
						}
						else
						{
							appendStringInfo(&buffer, " DEFAULT %s", defaultString);
						}
					}
				}
			}

			/* if this column has a not null constraint, append the constraint */
			if (attributeForm->attnotnull)
			{
				appendStringInfoString(&buffer, " NOT NULL");
			}

			if (attributeForm->attcollation != InvalidOid &&
				attributeForm->attcollation != DEFAULT_COLLATION_OID)
			{
				appendStringInfo(&buffer, " COLLATE %s", generate_collation_name(
									 attributeForm->attcollation));
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


		/* if an attribute or constraint has been printed, format properly */
		if (firstAttributePrinted || constraintIndex > 0)
		{
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
						 quote_identifier(checkConstraint->ccname));

		/* convert expression to node tree, and prepare deparse context */
		Node *checkNode = (Node *) stringToNode(checkConstraint->ccbin);
		List *checkContext = deparse_context_for(relationName, tableRelationId);

		/* deparse check constraint string */
		char *checkString = deparse_expression(checkNode, checkContext, false, false);

		appendStringInfoString(&buffer, "(");
		appendStringInfoString(&buffer, checkString);
		appendStringInfoString(&buffer, ")");
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	char relationKind = relation->rd_rel->relkind;
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(tableRelationId);
		ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

		char *serverName = foreignServer->servername;
		appendStringInfo(&buffer, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(&buffer, foreignTable->options);
	}
	else if (relationKind == RELKIND_PARTITIONED_TABLE)
	{
		char *partitioningInformation = GeneratePartitioningInformation(tableRelationId);
		appendStringInfo(&buffer, " PARTITION BY %s ", partitioningInformation);
	}

	/*
	 * Add table access methods when the table is configured with an
	 * access method
	 */
	if (accessMethod)
	{
		appendStringInfo(&buffer, " USING %s", quote_identifier(accessMethod));
	}
	else if (OidIsValid(relation->rd_rel->relam))
	{
		HeapTuple amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(
											  relation->rd_rel->relam));
		if (!HeapTupleIsValid(amTup))
		{
			elog(ERROR, "cache lookup failed for access method %u",
				 relation->rd_rel->relam);
		}
		Form_pg_am amForm = (Form_pg_am) GETSTRUCT(amTup);
		appendStringInfo(&buffer, " USING %s", quote_identifier(NameStr(amForm->amname)));
		ReleaseSysCache(amTup);
	}

	/*
	 * Add any reloptions (storage parameters) defined on the table in a WITH
	 * clause.
	 */
	{
		char *reloptions = flatten_reloptions(tableRelationId);
		if (reloptions)
		{
			appendStringInfo(&buffer, " WITH (%s)", reloptions);
			pfree(reloptions);
		}
	}

	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * EnsureRelationKindSupported errors out if the given relation is not supported
 * as a distributed relation.
 */
void
EnsureRelationKindSupported(Oid relationId)
{
	char relationKind = get_rel_relkind(relationId);
	if (!relationKind)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation with OID %d does not exist",
							   relationId)));
	}

	bool supportedRelationKind = RegularTable(relationId) ||
								 relationKind == RELKIND_FOREIGN_TABLE;

	/*
	 * Citus doesn't support bare inherited tables (i.e., not a partition or
	 * partitioned table)
	 */
	supportedRelationKind = supportedRelationKind && !(IsChildTable(relationId) ||
													   IsParentTable(relationId));

	if (!supportedRelationKind)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular, foreign or partitioned table",
							   relationName)));
	}
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
	List *columnOptionList = NIL;
	ListCell *columnOptionCell = NULL;
	bool firstOptionPrinted = false;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs, we open the relation,
	 * and use the relation's tuple descriptor to access attribute information.
	 * This is primarily to maintain symmetry with pg_get_tableschemadef.
	 */
	Relation relation = relation_open(tableRelationId, AccessShareLock);

	EnsureRelationKindSupported(tableRelationId);

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, check if column storage or
	 * statistics statements need to be printed.
	 */
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	if (tupleDescriptor->natts > MaxAttrNumber)
	{
		ereport(ERROR, (errmsg("bad number of tuple descriptor attributes")));
	}

	AttrNumber natts = tupleDescriptor->natts;
	for (AttrNumber attributeIndex = 0;
		 attributeIndex < natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);
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
			HeapTuple atttuple = SearchSysCache2(ATTNUM,
												 ObjectIdGetDatum(tableRelationId),
												 Int16GetDatum(attributeForm->attnum));
			if (!HeapTupleIsValid(atttuple))
			{
				elog(ERROR, "cache lookup failed for attribute %d of relation %u",
					 attributeForm->attnum, tableRelationId);
			}

			int32 targetAttstattarget = getAttstattarget_compat(atttuple);
			ReleaseSysCache(atttuple);
			if (targetAttstattarget >= 0)
			{
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STATISTICS %d",
								 targetAttstattarget);

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

		char *columnOptionStatement = (char *) lfirst(columnOptionCell);
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

	/* extend relation and index name using shard identifier */
	AppendShardIdToName(&(indexStmt->relation->relname), shardid);
	AppendShardIdToName(&(indexStmt->idxname), shardid);

	char *relationName = indexStmt->relation->relname;
	char *indexName = indexStmt->idxname;

	/* use extended shard name and transformed stmt for deparsing */
	List *deparseContext = deparse_context_for(relationName, distrelid);
	indexStmt = transformIndexStmt(distrelid, indexStmt, NULL);

	appendStringInfo(buffer, "CREATE %s INDEX %s %s %s ON %s %s USING %s ",
					 (indexStmt->unique ? "UNIQUE" : ""),
					 (indexStmt->concurrent ? "CONCURRENTLY" : ""),
					 (indexStmt->if_not_exists ? "IF NOT EXISTS" : ""),
					 quote_identifier(indexName),
					 (indexStmt->relation->inh ? "" : "ONLY"),
					 quote_qualified_identifier(indexStmt->relation->schemaname,
												relationName),
					 indexStmt->accessMethod);

	/*
	 * Switch to empty search_path to deparse_index_columns to produce fully-
	 * qualified names in expressions.
	 */
	int saveNestLevel = PushEmptySearchPath();

	/* index column or expression list begins here */
	appendStringInfoChar(buffer, '(');
	deparse_index_columns(buffer, indexStmt->indexParams, deparseContext);
	appendStringInfoString(buffer, ") ");

	/* column/expressions for INCLUDE list */
	if (indexStmt->indexIncludingParams != NIL)
	{
		appendStringInfoString(buffer, "INCLUDE (");
		deparse_index_columns(buffer, indexStmt->indexIncludingParams, deparseContext);
		appendStringInfoString(buffer, ") ");
	}

	if (indexStmt->nulls_not_distinct)
	{
		appendStringInfoString(buffer, "NULLS NOT DISTINCT ");
	}

	if (indexStmt->options != NIL)
	{
		appendStringInfoString(buffer, "WITH (");
		AppendStorageParametersToString(buffer, indexStmt->options);
		appendStringInfoString(buffer, ") ");
	}

	if (indexStmt->whereClause != NULL)
	{
		appendStringInfo(buffer, "WHERE %s", deparse_expression(indexStmt->whereClause,
																deparseContext, false,
																false));
	}

	/* revert back to original search_path */
	PopEmptySearchPath(saveNestLevel);
}


/*
 * deparse_shard_reindex_statement uses the provided REINDEX node, dist.
 * relation, and shard identifier to populate a provided buffer with a string
 * representation of a shard-extended version of that command.
 */
void
deparse_shard_reindex_statement(ReindexStmt *origStmt, Oid distrelid, int64 shardid,
								StringInfo buffer)
{
	ReindexStmt *reindexStmt = copyObject(origStmt); /* copy to avoid modifications */
	char *relationName = NULL;
	const char *concurrentlyString =
		IsReindexWithParam_compat(reindexStmt, "concurrently") ? "CONCURRENTLY " : "";


	if (reindexStmt->kind == REINDEX_OBJECT_INDEX ||
		reindexStmt->kind == REINDEX_OBJECT_TABLE)
	{
		/* extend relation and index name using shard identifier */
		AppendShardIdToName(&(reindexStmt->relation->relname), shardid);

		relationName = reindexStmt->relation->relname;
	}

	appendStringInfoString(buffer, "REINDEX ");
	AddVacuumParams(reindexStmt, buffer);

	switch (reindexStmt->kind)
	{
		case REINDEX_OBJECT_INDEX:
		{
			appendStringInfo(buffer, "INDEX %s%s", concurrentlyString,
							 quote_qualified_identifier(reindexStmt->relation->schemaname,
														relationName));
			break;
		}

		case REINDEX_OBJECT_TABLE:
		{
			appendStringInfo(buffer, "TABLE %s%s", concurrentlyString,
							 quote_qualified_identifier(reindexStmt->relation->schemaname,
														relationName));
			break;
		}

		case REINDEX_OBJECT_SCHEMA:
		{
			appendStringInfo(buffer, "SCHEMA %s%s", concurrentlyString,
							 quote_identifier(reindexStmt->name));
			break;
		}

		case REINDEX_OBJECT_SYSTEM:
		{
			appendStringInfo(buffer, "SYSTEM %s%s", concurrentlyString,
							 quote_identifier(reindexStmt->name));
			break;
		}

		case REINDEX_OBJECT_DATABASE:
		{
			appendStringInfo(buffer, "DATABASE %s%s", concurrentlyString,
							 quote_identifier(reindexStmt->name));
			break;
		}
	}
}


/*
 * IsReindexWithParam_compat returns true if the given parameter
 * exists for the given reindexStmt.
 */
bool
IsReindexWithParam_compat(ReindexStmt *reindexStmt, char *param)
{
	DefElem *opt = NULL;
	foreach_declared_ptr(opt, reindexStmt->params)
	{
		if (strcmp(opt->defname, param) == 0)
		{
			return defGetBoolean(opt);
		}
	}
	return false;
}


/*
 * AddVacuumParams adds vacuum params to the given buffer.
 */
static void
AddVacuumParams(ReindexStmt *reindexStmt, StringInfo buffer)
{
	StringInfo temp = makeStringInfo();
	if (IsReindexWithParam_compat(reindexStmt, "verbose"))
	{
		appendStringInfoString(temp, "VERBOSE");
	}

	char *tableSpaceName = NULL;
	DefElem *opt = NULL;
	foreach_declared_ptr(opt, reindexStmt->params)
	{
		if (strcmp(opt->defname, "tablespace") == 0)
		{
			tableSpaceName = defGetString(opt);
			break;
		}
	}

	if (tableSpaceName)
	{
		if (temp->len > 0)
		{
			appendStringInfo(temp, ", TABLESPACE %s", tableSpaceName);
		}
		else
		{
			appendStringInfo(temp, "TABLESPACE %s", tableSpaceName);
		}
	}

	if (temp->len > 0)
	{
		appendStringInfo(buffer, "(%s) ", temp->data);
	}
}


/* deparse_index_columns appends index or include parameters to the provided buffer */
static void
deparse_index_columns(StringInfo buffer, List *indexParameterList, List *deparseContext)
{
	ListCell *indexParameterCell = NULL;
	foreach(indexParameterCell, indexParameterList)
	{
		IndexElem *indexElement = (IndexElem *) lfirst(indexParameterCell);

		/* use commas to separate subsequent elements */
		if (indexParameterCell != list_head(indexParameterList))
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

		/* Commit on postgres: 911e70207703799605f5a0e8aad9f06cff067c63*/
		if (indexElement->opclassopts != NIL)
		{
			appendStringInfoString(buffer, "(");
			AppendStorageParametersToString(buffer, indexElement->opclassopts);
			appendStringInfoString(buffer, ") ");
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
}


/*
 * pg_get_indexclusterdef_string returns the definition of a cluster statement
 * for given index. The function returns null if the table is not clustered on
 * given index.
 */
char *
pg_get_indexclusterdef_string(Oid indexRelationId)
{
	StringInfoData buffer = { NULL, 0, 0, 0 };

	HeapTuple indexTuple = SearchSysCache(INDEXRELID, ObjectIdGetDatum(indexRelationId),
										  0, 0, 0);
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for index %u", indexRelationId)));
	}

	Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	Oid tableRelationId = indexForm->indrelid;

	/* check if the table is clustered on this index */
	if (indexForm->indisclustered)
	{
		char *qualifiedRelationName =
			generate_qualified_relation_name(tableRelationId);
		char *indexName = get_rel_name(indexRelationId); /* needs to be quoted */

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "ALTER TABLE %s CLUSTER ON %s",
						 qualifiedRelationName, quote_identifier(indexName));
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
	List *defs = NIL;
	bool isNull = false;

	Relation relation = relation_open(relationId, AccessShareLock);
	char *relationName = generate_relation_name(relationId, NIL);

	initStringInfo(&buffer);

	/* lookup all table level grants */
	HeapTuple classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(classTuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist",
						relationId)));
	}

	Datum aclDatum = SysCacheGetAttr(RELOID, classTuple, Anum_pg_class_relacl,
							   &isNull);

	ReleaseSysCache(classTuple);

	if (!isNull)
	{

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

		Acl *acl = DatumGetAclP(aclDatum);
		AclItem *aidat = ACL_DAT(acl);

		int offtype = -1;
		int i = 0;
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

					HeapTuple htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aidata->ai_grantee));
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
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
	{
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
	char *relname = NameStr(reltup->relname);

	char *nspname = get_namespace_name(reltup->relnamespace);
	if (!nspname)
	{
		elog(ERROR, "cache lookup failed for namespace %u",
			 reltup->relnamespace);
	}

	char *result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}


/*
 * AppendOptionListToString converts the option list to its textual format, and
 * appends this text to the given string buffer.
 */
void
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
 * AppendStorageParametersToString converts the storage parameter list to its
 * textual format, and appends this text to the given string buffer.
 */
static void
AppendStorageParametersToString(StringInfo stringBuffer, List *optionList)
{
	ListCell *optionCell = NULL;
	bool firstOptionPrinted = false;

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

		appendStringInfo(stringBuffer, "%s = %s ",
						 quote_identifier(optionName),
						 quote_literal_cstr(optionValue));
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
#if PG_VERSION_NUM >= PG_VERSION_17
		case ACL_MAINTAIN:
			return "MAINTAIN";
#endif
		default:
			elog(ERROR, "unrecognized aclright: %d", aclright);
			return NULL;
	}
	/* *INDENT-ON* */
}


/*
 * contain_nextval_expression_walker walks over expression tree and returns
 * true if it contains call to 'nextval' function or it has an identity column.
 */
bool
contain_nextval_expression_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	/* check if the node contains an identity column */
	if (IsA(node, NextValueExpr))
	{
		return true;
	}

	/* check if the node contains call to 'nextval' */
	if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == F_NEXTVAL)
		{
			return true;
		}
	}
	return expression_tree_walker(node, contain_nextval_expression_walker, context);
}


/*
 * pg_get_replica_identity_command function returns the required ALTER .. TABLE
 * command to define the replica identity.
 */
char *
pg_get_replica_identity_command(Oid tableRelationId)
{
	StringInfo buf = makeStringInfo();

	Relation relation = table_open(tableRelationId, AccessShareLock);

	char replicaIdentity = relation->rd_rel->relreplident;

	char *relationName = generate_qualified_relation_name(tableRelationId);

	if (replicaIdentity == REPLICA_IDENTITY_INDEX)
	{
		Oid indexId = RelationGetReplicaIndex(relation);

		if (OidIsValid(indexId))
		{
			appendStringInfo(buf, "ALTER TABLE %s REPLICA IDENTITY USING INDEX %s ",
							 relationName,
							 quote_identifier(get_rel_name(indexId)));
		}
	}
	else if (replicaIdentity == REPLICA_IDENTITY_NOTHING)
	{
		appendStringInfo(buf, "ALTER TABLE %s REPLICA IDENTITY NOTHING",
						 relationName);
	}
	else if (replicaIdentity == REPLICA_IDENTITY_FULL)
	{
		appendStringInfo(buf, "ALTER TABLE %s REPLICA IDENTITY FULL",
						 relationName);
	}

	table_close(relation, AccessShareLock);

	return (buf->len > 0) ? buf->data : NULL;
}


/*
 * pg_get_row_level_security_commands function returns the required ALTER .. TABLE
 * commands to define the row level security settings for a relation.
 */
List *
pg_get_row_level_security_commands(Oid relationId)
{
	StringInfoData buffer;
	List *commands = NIL;

	initStringInfo(&buffer);

	Relation relation = table_open(relationId, AccessShareLock);

	if (relation->rd_rel->relrowsecurity)
	{
		char *relationName = generate_qualified_relation_name(relationId);

		appendStringInfo(&buffer, "ALTER TABLE %s ENABLE ROW LEVEL SECURITY",
						 relationName);
		commands = lappend(commands, pstrdup(buffer.data));
		resetStringInfo(&buffer);
	}

	if (relation->rd_rel->relforcerowsecurity)
	{
		char *relationName = generate_qualified_relation_name(relationId);

		appendStringInfo(&buffer, "ALTER TABLE %s FORCE ROW LEVEL SECURITY",
						 relationName);
		commands = lappend(commands, pstrdup(buffer.data));
		resetStringInfo(&buffer);
	}

	table_close(relation, AccessShareLock);

	return commands;
}


/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 *
 * This function comes from PostgreSQL source code in
 * src/backend/utils/adt/ruleutils.c
 */
char *
flatten_reloptions(Oid relid)
{
	char *result = NULL;
	bool isnull;

	HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}

	Datum reloptions = SysCacheGetAttr(RELOID, tuple,
									   Anum_pg_class_reloptions, &isnull);
	if (!isnull)
	{
		StringInfoData buf;
		Datum *options;
		int noptions;
		int i;

		initStringInfo(&buf);

		deconstruct_array(DatumGetArrayTypeP(reloptions),
						  TEXTOID, -1, false, 'i',
						  &options, NULL, &noptions);

		for (i = 0; i < noptions; i++)
		{
			char *option = TextDatumGetCString(options[i]);
			char *value;

			/*
			 * Each array element should have the form name=value.  If the "="
			 * is missing for some reason, treat it like an empty value.
			 */
			char *name = option;
			char *separator = strchr(option, '=');
			if (separator)
			{
				*separator = '\0';
				value = separator + 1;
			}
			else
			{
				value = "";
			}

			if (i > 0)
			{
				appendStringInfoString(&buf, ", ");
			}
			appendStringInfo(&buf, "%s=", quote_identifier(name));

			/*
			 * In general we need to quote the value; but to avoid unnecessary
			 * clutter, do not quote if it is an identifier that would not
			 * need quoting.  (We could also allow numbers, but that is a bit
			 * trickier than it looks --- for example, are leading zeroes
			 * significant?  We don't want to assume very much here about what
			 * custom reloptions might mean.)
			 */
			if (quote_identifier(value) == value)
			{
				appendStringInfoString(&buf, value);
			}
			else
			{
				simple_quote_literal(&buf, value);
			}

			pfree(option);
		}

		result = buf.data;
	}

	ReleaseSysCache(tuple);

	return result;
}


/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 *
 * This function comes from PostgreSQL source code in
 * src/backend/utils/adt/ruleutils.c
 */
static void
simple_quote_literal(StringInfo buf, const char *val)
{
	/*
	 * We form the string literal according to the prevailing setting of
	 * standard_conforming_strings; we never use E''. User is responsible for
	 * making sure result is used correctly.
	 */
	appendStringInfoChar(buf, '\'');
	for (const char *valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		if (SQL_STR_DOUBLE(ch, !standard_conforming_strings))
		{
			appendStringInfoChar(buf, ch);
		}
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}


/*
 * RoleSpecString resolves the role specification to its string form that is suitable for transport to a worker node.
 * This function resolves the following identifiers from the current context so they are safe to transfer.
 *
 * CURRENT_USER - resolved to the user name of the current role being used
 * SESSION_USER - resolved to the user name of the user that opened the session
 * CURRENT_ROLE - same as CURRENT_USER, resolved to the user name of the current role being used
 * Postgres treats CURRENT_ROLE is equivalent to CURRENT_USER, and we follow the same approach.
 *
 * withQuoteIdentifier is used, because if the results will be used in a query the quotes are needed but if not there
 * should not be extra quotes.
 */
const char *
RoleSpecString(RoleSpec *spec, bool withQuoteIdentifier)
{
	switch (spec->roletype)
	{
		case ROLESPEC_CSTRING:
		{
			return withQuoteIdentifier ?
				   quote_identifier(spec->rolename) :
				   spec->rolename;
		}

		case ROLESPEC_CURRENT_ROLE:
		case ROLESPEC_CURRENT_USER:
		{
			return withQuoteIdentifier ?
				   quote_identifier(GetUserNameFromId(GetUserId(), false)) :
				   GetUserNameFromId(GetUserId(), false);
		}

		case ROLESPEC_SESSION_USER:
		{
			return withQuoteIdentifier ?
				   quote_identifier(GetUserNameFromId(GetSessionUserId(), false)) :
				   GetUserNameFromId(GetSessionUserId(), false);
		}

		case ROLESPEC_PUBLIC:
		{
			return "PUBLIC";
		}

		default:
		{
			elog(ERROR, "unexpected role type %d", spec->roletype);
		}
	}
}
