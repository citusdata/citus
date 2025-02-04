/*-------------------------------------------------------------------------
 *
 * deparser.h
 *	  Used when deparsing any ddl parsetree into its sql from.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPARSER_H
#define CITUS_DEPARSER_H

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

/* forward declarations for format_collate.c */
/* Control flags for FormatCollateExtended, compatible with format_type_extended */
#define FORMAT_COLLATE_ALLOW_INVALID 0x02       /* allow invalid types */
#define FORMAT_COLLATE_FORCE_QUALIFY 0x04       /* force qualification of collate */
extern char * FormatCollateBEQualified(Oid collate_oid);
extern char * FormatCollateExtended(Oid collid, bits16 flags);

extern void AssertObjectTypeIsFunctional(ObjectType type);

extern void QualifyTreeNode(Node *stmt);
extern char * DeparseTreeNode(Node *stmt);
extern List * DeparseTreeNodes(List *stmts);

/* forward declarations for qualify_aggregate_stmts.c */
extern void QualifyDefineAggregateStmt(Node *node);

/* forward declarations for deparse_attribute_stmts.c */
extern char * DeparseRenameAttributeStmt(Node *);

/* forward declarations for deparse_collation_stmts.c */
extern char * DeparseDropCollationStmt(Node *stmt);
extern char * DeparseRenameCollationStmt(Node *stmt);
extern char * DeparseAlterCollationSchemaStmt(Node *stmt);
extern char * DeparseAlterCollationOwnerStmt(Node *stmt);

extern void QualifyDropCollationStmt(Node *stmt);
extern void QualifyRenameCollationStmt(Node *stmt);
extern void QualifyAlterCollationSchemaStmt(Node *stmt);
extern void QualifyAlterCollationOwnerStmt(Node *stmt);

/* forward declarations for deparse_domain_stmts.c */
extern char * DeparseCreateDomainStmt(Node *node);
extern char * DeparseDropDomainStmt(Node *node);
extern char * DeparseAlterDomainStmt(Node *node);
extern char * DeparseDomainRenameConstraintStmt(Node *node);
extern char * DeparseAlterDomainOwnerStmt(Node *node);
extern char * DeparseRenameDomainStmt(Node *node);
extern char * DeparseAlterDomainSchemaStmt(Node *node);

extern void QualifyCreateDomainStmt(Node *node);
extern void QualifyDropDomainStmt(Node *node);
extern void QualifyAlterDomainStmt(Node *node);
extern void QualifyDomainRenameConstraintStmt(Node *node);
extern void QualifyAlterDomainOwnerStmt(Node *node);
extern void QualifyRenameDomainStmt(Node *node);
extern void QualifyAlterDomainSchemaStmt(Node *node);

/* forward declarations for deparse_foreign_data_wrapper_stmts.c */
extern char * DeparseGrantOnFDWStmt(Node *node);

/* forward declarations for deparse_foreign_server_stmts.c */
extern char * DeparseCreateForeignServerStmt(Node *node);
extern char * DeparseAlterForeignServerStmt(Node *node);
extern char * DeparseAlterForeignServerRenameStmt(Node *node);
extern char * DeparseAlterForeignServerOwnerStmt(Node *node);
extern char * DeparseDropForeignServerStmt(Node *node);
extern char * DeparseGrantOnForeignServerStmt(Node *node);

/* forward declarations for deparse_table_stmts.c */
extern char * DeparseAlterTableSchemaStmt(Node *stmt);
extern char * DeparseAlterTableStmt(Node *node);

extern void QualifyAlterTableSchemaStmt(Node *stmt);

/* forward declarations for deparse_text_search.c */
extern char * DeparseAlterTextSearchConfigurationOwnerStmt(Node *node);
extern char * DeparseAlterTextSearchConfigurationSchemaStmt(Node *node);
extern char * DeparseAlterTextSearchConfigurationStmt(Node *node);
extern char * DeparseAlterTextSearchDictionaryOwnerStmt(Node *node);
extern char * DeparseAlterTextSearchDictionarySchemaStmt(Node *node);
extern char * DeparseAlterTextSearchDictionaryStmt(Node *node);
extern char * DeparseCreateTextSearchConfigurationStmt(Node *node);
extern char * DeparseCreateTextSearchDictionaryStmt(Node *node);
extern char * DeparseDropTextSearchConfigurationStmt(Node *node);
extern char * DeparseDropTextSearchDictionaryStmt(Node *node);
extern char * DeparseRenameTextSearchConfigurationStmt(Node *node);
extern char * DeparseRenameTextSearchDictionaryStmt(Node *node);
extern char * DeparseTextSearchConfigurationCommentStmt(Node *node);
extern char * DeparseTextSearchDictionaryCommentStmt(Node *node);

/* forward declarations for deparse_schema_stmts.c */
extern char * DeparseCreateSchemaStmt(Node *node);
extern char * DeparseDropSchemaStmt(Node *node);
extern char * DeparseGrantOnSchemaStmt(Node *stmt);
extern char * DeparseAlterSchemaRenameStmt(Node *stmt);
extern char * DeparseAlterSchemaOwnerStmt(Node *node);

extern void AppendGrantPrivileges(StringInfo buf, GrantStmt *stmt);
extern void AppendGrantGrantees(StringInfo buf, GrantStmt *stmt);
extern void AppendWithGrantOption(StringInfo buf, GrantStmt *stmt);
extern void AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt);
extern void AppendGrantRestrictAndCascadeForRoleSpec(StringInfo buf, DropBehavior
													 behavior, bool isGrant);
extern void AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt *stmt);
extern void AppendGrantedByInGrantForRoleSpec(StringInfo buf, RoleSpec *grantor, bool
											  isGrant);
extern void AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt);

extern void AppendGrantSharedPrefix(StringInfo buf, GrantStmt *stmt);
extern void AppendGrantSharedSuffix(StringInfo buf, GrantStmt *stmt);

extern void AppendColumnNameList(StringInfo buf, List *columns);

/* Common deparser utils */

typedef struct DefElemOptionFormat
{
	char *name;
	char *format;
	int type;
} DefElemOptionFormat;

typedef enum OptionFormatType
{
	OPTION_FORMAT_STRING,
	OPTION_FORMAT_LITERAL_CSTR,
	OPTION_FORMAT_BOOLEAN,
	OPTION_FORMAT_INTEGER
} OptionFormatType;


extern void DefElemOptionToStatement(StringInfo buf, DefElem *option,
									 const DefElemOptionFormat *opt_formats,
									 int opt_formats_len);

/* forward declarations for deparse_comment_stmts.c */
extern char * DeparseCommentStmt(Node *node);


/* forward declarations for deparse_statistics_stmts.c */
extern char * DeparseCreateStatisticsStmt(Node *node);
extern char * DeparseDropStatisticsStmt(List *nameList, bool ifExists);
extern char * DeparseAlterStatisticsRenameStmt(Node *node);
extern char * DeparseAlterStatisticsSchemaStmt(Node *node);
extern char * DeparseAlterStatisticsStmt(Node *node);
extern char * DeparseAlterStatisticsOwnerStmt(Node *node);

extern void QualifyCreateStatisticsStmt(Node *node);
extern void QualifyDropStatisticsStmt(Node *node);
extern void QualifyAlterStatisticsRenameStmt(Node *node);
extern void QualifyAlterStatisticsSchemaStmt(Node *node);
extern void QualifyAlterStatisticsStmt(Node *node);
extern void QualifyAlterStatisticsOwnerStmt(Node *node);

/* forward declarations for deparse_type_stmts.c */
extern char * DeparseCompositeTypeStmt(Node *stmt);
extern char * DeparseCreateEnumStmt(Node *stmt);
extern char * DeparseDropTypeStmt(Node *stmt);
extern char * DeparseAlterEnumStmt(Node *stmt);
extern char * DeparseAlterTypeStmt(Node *stmt);
extern char * DeparseRenameTypeStmt(Node *stmt);
extern char * DeparseRenameTypeAttributeStmt(Node *stmt);
extern char * DeparseAlterTypeSchemaStmt(Node *stmt);
extern char * DeparseAlterTypeOwnerStmt(Node *stmt);

extern void QualifyRenameAttributeStmt(Node *stmt);
extern void QualifyRenameTypeStmt(Node *stmt);
extern void QualifyRenameTypeAttributeStmt(Node *stmt);
extern void QualifyAlterEnumStmt(Node *stmt);
extern void QualifyAlterTypeStmt(Node *stmt);
extern void QualifyCompositeTypeStmt(Node *stmt);
extern void QualifyCreateEnumStmt(Node *stmt);
extern void QualifyAlterTypeSchemaStmt(Node *stmt);
extern void QualifyAlterTypeOwnerStmt(Node *stmt);

extern char * GetTypeNamespaceNameByNameList(List *names);
extern Oid TypeOidGetNamespaceOid(Oid typeOid);

extern List * GetObjectAddressListFromParseTree(Node *parseTree, bool missing_ok, bool
												isPostprocess);
extern List * RenameAttributeStmtObjectAddress(Node *stmt, bool missing_ok, bool
											   isPostprocess);

/* forward declarations for deparse_view_stmts.c */
extern void QualifyDropViewStmt(Node *node);
extern void QualifyAlterViewStmt(Node *node);
extern void QualifyRenameViewStmt(Node *node);
extern void QualifyAlterViewSchemaStmt(Node *node);
extern char * DeparseRenameViewStmt(Node *stmt);
extern char * DeparseAlterViewStmt(Node *node);
extern char * DeparseDropViewStmt(Node *node);
extern char * DeparseAlterViewSchemaStmt(Node *node);


/* forward declarations for deparse_function_stmts.c */
extern bool isFunction(ObjectType objectType);

extern char * DeparseDropFunctionStmt(Node *stmt);
extern char * DeparseAlterFunctionStmt(Node *stmt);

extern char * DeparseRenameFunctionStmt(Node *stmt);
extern char * DeparseAlterFunctionSchemaStmt(Node *stmt);
extern char * DeparseAlterFunctionOwnerStmt(Node *stmt);
extern char * DeparseAlterFunctionDependsStmt(Node *stmt);

extern char * DeparseGrantOnFunctionStmt(Node *node);

extern void AppendVariableSet(StringInfo buf, VariableSetStmt *setStmt);

extern void QualifyAlterFunctionStmt(Node *stmt);
extern void QualifyRenameFunctionStmt(Node *stmt);
extern void QualifyAlterFunctionSchemaStmt(Node *stmt);
extern void QualifyAlterFunctionOwnerStmt(Node *stmt);
extern void QualifyAlterFunctionDependsStmt(Node *stmt);

/* forward declarations for deparse_role_stmts.c */
extern char * DeparseAlterRoleStmt(Node *stmt);
extern char * DeparseAlterRoleSetStmt(Node *stmt);
extern char * DeparseRenameRoleStmt(Node *stmt);

extern List * MakeSetStatementArguments(char *configurationName,
										char *configurationValue);
extern void QualifyAlterRoleSetStmt(Node *stmt);
extern char * DeparseCreateRoleStmt(Node *stmt);
extern char * DeparseDropRoleStmt(Node *stmt);
extern char * DeparseGrantRoleStmt(Node *stmt);
extern char * DeparseReassignOwnedStmt(Node *node);

/* forward declarations for deparse_owned_stmts.c */
extern char * DeparseDropOwnedStmt(Node *node);

/* forward declarations for deparse_extension_stmts.c */
extern DefElem * GetExtensionOption(List *extensionOptions,
									const char *defname);
extern char * DeparseCreateExtensionStmt(Node *stmt);
extern char * DeparseDropExtensionStmt(Node *stmt);
extern char * DeparseAlterExtensionSchemaStmt(Node *stmt);
extern char * DeparseAlterExtensionStmt(Node *stmt);

/* forward declarations for deparse_database_stmts.c */
extern char * DeparseAlterDatabaseOwnerStmt(Node *node);
extern char * DeparseGrantOnDatabaseStmt(Node *node);
extern char * DeparseAlterDatabaseStmt(Node *node);
extern char * DeparseAlterDatabaseRefreshCollStmt(Node *node);
extern char * DeparseAlterDatabaseSetStmt(Node *node);
extern char * DeparseCreateDatabaseStmt(Node *node);
extern char * DeparseDropDatabaseStmt(Node *node);
extern char * DeparseAlterDatabaseRenameStmt(Node *node);


/* forward declaration for deparse_publication_stmts.c */
extern char * DeparseCreatePublicationStmt(Node *stmt);
extern char * DeparseCreatePublicationStmtExtended(Node *node,
												   bool whereClauseNeedsTransform,
												   bool includeLocalTables);
extern char * DeparseAlterPublicationStmt(Node *stmt);
extern char * DeparseAlterPublicationStmtExtended(Node *stmt,
												  bool whereClauseNeedsTransform,
												  bool includeLocalTables);
extern char * DeparseAlterPublicationOwnerStmt(Node *stmt);
extern char * DeparseAlterPublicationSchemaStmt(Node *node);
extern char * DeparseDropPublicationStmt(Node *stmt);
extern char * DeparseRenamePublicationStmt(Node *node);

extern void QualifyCreatePublicationStmt(Node *node);
extern void QualifyAlterPublicationStmt(Node *node);

/* forward declatations for deparse_text_search_stmts.c */
extern void QualifyAlterTextSearchConfigurationOwnerStmt(Node *node);
extern void QualifyAlterTextSearchConfigurationSchemaStmt(Node *node);
extern void QualifyAlterTextSearchConfigurationStmt(Node *node);
extern void QualifyAlterTextSearchDictionaryOwnerStmt(Node *node);
extern void QualifyAlterTextSearchDictionarySchemaStmt(Node *node);
extern void QualifyAlterTextSearchDictionaryStmt(Node *node);
extern void QualifyDropTextSearchConfigurationStmt(Node *node);
extern void QualifyDropTextSearchDictionaryStmt(Node *node);
extern void QualifyRenameTextSearchConfigurationStmt(Node *node);
extern void QualifyRenameTextSearchDictionaryStmt(Node *node);
extern void QualifyTextSearchConfigurationCommentStmt(Node *node);
extern void QualifyTextSearchDictionaryCommentStmt(Node *node);

/* forward declarations for deparse_seclabel_stmts.c */
extern char * DeparseSecLabelStmt(Node *node);

/* forward declarations for deparse_sequence_stmts.c */
extern char * DeparseDropSequenceStmt(Node *node);
extern char * DeparseRenameSequenceStmt(Node *node);
extern char * DeparseAlterSequenceSchemaStmt(Node *node);
extern char * DeparseAlterSequenceOwnerStmt(Node *node);
#if (PG_VERSION_NUM >= PG_VERSION_15)
extern char * DeparseAlterSequencePersistenceStmt(Node *node);
#endif
extern char * DeparseGrantOnSequenceStmt(Node *node);

/* forward declarations for qualify_sequence_stmt.c */
extern void QualifyRenameSequenceStmt(Node *node);
extern void QualifyDropSequenceStmt(Node *node);
extern void QualifyAlterSequenceSchemaStmt(Node *node);
extern void QualifyAlterSequenceOwnerStmt(Node *node);
#if (PG_VERSION_NUM >= PG_VERSION_15)
extern void QualifyAlterSequencePersistenceStmt(Node *node);
#endif
extern void QualifyGrantOnSequenceStmt(Node *node);

#endif /* CITUS_DEPARSER_H */
