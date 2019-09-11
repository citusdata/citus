/*-------------------------------------------------------------------------
 *
 * deparser.h
 *	  Used when deparsing any ddl parsetree into its sql from.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPARSER_H
#define CITUS_DEPARSER_H

#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "catalog/objectaddress.h"

/* forward declarations for format_collate.c */
/* Control flags for FormatCollateExtended, compatible with format_type_extended */
#define FORMAT_COLLATE_ALLOW_INVALID 0x02       /* allow invalid types */
#define FORMAT_COLLATE_FORCE_QUALIFY 0x04       /* force qualification of collate */
extern char * FormatCollateBE(Oid collate_oid);
extern char * FormatCollateBEQualified(Oid collate_oid);
extern char * FormatCollateExtended(Oid collid, bits16 flags);

extern void AssertObjectTypeIsFunctional(ObjectType type);

extern void QualifyTreeNode(Node *stmt);
extern char * DeparseTreeNode(Node *stmt);

/* forward declarations for deparse_collation_stmts.c */
extern char * DeparseDropCollationStmt(DropStmt *stmt);
extern char * DeparseRenameCollationStmt(RenameStmt *stmt);
extern char * DeparseAlterCollationSchemaStmt(AlterObjectSchemaStmt *stmt);
extern char * DeparseAlterCollationOwnerStmt(AlterOwnerStmt *stmt);

extern void QualifyDropCollationStmt(DropStmt *stmt);
extern void QualifyRenameCollationStmt(RenameStmt *stmt);
extern void QualifyAlterCollationSchemaStmt(AlterObjectSchemaStmt *stmt);
extern void QualifyAlterCollationOwnerStmt(AlterOwnerStmt *stmt);

/* forward declarations for deparse_type_stmts.c */
extern char * DeparseCompositeTypeStmt(CompositeTypeStmt *stmt);
extern char * DeparseCreateEnumStmt(CreateEnumStmt *stmt);
extern char * DeparseDropTypeStmt(DropStmt *stmt);
extern char * DeparseAlterEnumStmt(AlterEnumStmt *stmt);
extern char * DeparseAlterTypeStmt(AlterTableStmt *stmt);
extern char * DeparseRenameTypeStmt(RenameStmt *stmt);
extern char * DeparseRenameTypeAttributeStmt(RenameStmt *stmt);
extern char * DeparseAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt);
extern char * DeparseAlterTypeOwnerStmt(AlterOwnerStmt *stmt);

extern void QualifyRenameTypeStmt(RenameStmt *stmt);
extern void QualifyRenameTypeAttributeStmt(RenameStmt *stmt);
extern void QualifyAlterEnumStmt(AlterEnumStmt *stmt);
extern void QualifyAlterTypeStmt(AlterTableStmt *stmt);
extern void QualifyCompositeTypeStmt(CompositeTypeStmt *stmt);
extern void QualifyCreateEnumStmt(CreateEnumStmt *stmt);
extern void QualifyAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt);
extern void QualifyAlterTypeOwnerStmt(AlterOwnerStmt *stmt);

extern ObjectAddress * GetObjectAddressFromParseTree(Node *parseTree, bool missing_ok);

/* forward declarations for deparse_function_stmts.c */
extern char * DeparseDropFunctionStmt(DropStmt *stmt);
extern char * DeparseAlterFunctionStmt(AlterFunctionStmt *stmt);

extern char * DeparseRenameFunctionStmt(RenameStmt *stmt);
extern char * DeparseAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt);
extern char * DeparseAlterFunctionOwnerStmt(AlterOwnerStmt *stmt);
extern char * DeparseAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt);

extern void QualifyAlterFunctionStmt(AlterFunctionStmt *stmt);
extern void QualifyRenameFunctionStmt(RenameStmt *stmt);
extern void QualifyAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt);
extern void QualifyAlterFunctionOwnerStmt(AlterOwnerStmt *stmt);
extern void QualifyAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt);

/* forward declarations for deparse_role_stmts.c */
extern char * DeparseAlterRoleStmt(AlterRoleStmt *stmt);

/* forward declarations for deparse_extension_stmts.c */
extern Value * GetExtensionOption(List *extensionOptions, const
								  char *defname);
extern char * DeparseCreateExtensionStmt(CreateExtensionStmt *stmt);
extern char * DeparseDropExtensionStmt(DropStmt *stmt);
extern char * DeparseAlterExtensionSchemaStmt(
	AlterObjectSchemaStmt *alterExtensionSchemaStmt);
extern char * DeparseAlterExtensionStmt(AlterExtensionStmt *alterExtensionStmt);

#endif /* CITUS_DEPARSER_H */
