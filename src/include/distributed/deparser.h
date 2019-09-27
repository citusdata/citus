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
extern const char * DeparseTreeNode(Node *stmt);

extern const char * DeparseCompositeTypeStmt(CompositeTypeStmt *stmt);
extern const char * DeparseCreateEnumStmt(CreateEnumStmt *stmt);
extern const char * DeparseDropTypeStmt(DropStmt *stmt);
extern const char * DeparseAlterEnumStmt(AlterEnumStmt *stmt);
extern const char * DeparseAlterTypeStmt(AlterTableStmt *stmt);
extern const char * DeparseRenameTypeStmt(RenameStmt *stmt);
extern const char * DeparseRenameTypeAttributeStmt(RenameStmt *stmt);
extern const char * DeparseAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt);
extern const char * DeparseAlterTypeOwnerStmt(AlterOwnerStmt *stmt);

extern void QualifyRenameTypeStmt(RenameStmt *stmt);
extern void QualifyRenameTypeAttributeStmt(RenameStmt *stmt);
extern void QualifyAlterEnumStmt(AlterEnumStmt *stmt);
extern void QualifyAlterTypeStmt(AlterTableStmt *stmt);
extern void QualifyCompositeTypeStmt(CompositeTypeStmt *stmt);
extern void QualifyCreateEnumStmt(CreateEnumStmt *stmt);
extern void QualifyAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt);
extern void QualifyAlterTypeOwnerStmt(AlterOwnerStmt *stmt);

extern const ObjectAddress * GetObjectAddressFromParseTree(Node *parseTree, bool
														   missing_ok);

/* forward declarations for deparse_function_stmts.c */
extern const char * DeparseDropFunctionStmt(DropStmt *stmt);
extern const char * DeparseAlterFunctionStmt(AlterFunctionStmt *stmt);

extern const char * DeparseRenameFunctionStmt(RenameStmt *stmt);
extern const char * DeparseAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt);
extern const char * DeparseAlterFunctionOwnerStmt(AlterOwnerStmt *stmt);
extern const char * DeparseAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt);

extern void QualifyAlterFunctionStmt(AlterFunctionStmt *stmt);
extern void QualifyRenameFunctionStmt(RenameStmt *stmt);
extern void QualifyAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt);
extern void QualifyAlterFunctionOwnerStmt(AlterOwnerStmt *stmt);
extern void QualifyAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt);

#endif /* CITUS_DEPARSER_H */
