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
/* Control flags for format_collate_extended, compatible with format_type_extended */
#define FORMAT_COLLATE_ALLOW_INVALID 0x02       /* allow invalid types */
#define FORMAT_COLLATE_FORCE_QUALIFY 0x04       /* force qualification of collate */
extern char * format_collate_be(Oid type_oid);
extern char * format_collate_be_qualified(Oid type_oid);
extern char * format_collate_extended(Oid collid, bits16 flags);

extern void QualifyTreeNode(Node *stmt);
extern const char * DeparseTreeNode(Node *stmt);

extern const char * deparse_composite_type_stmt(CompositeTypeStmt *stmt);
extern const char * deparse_create_enum_stmt(CreateEnumStmt *stmt);
extern const char * deparse_drop_type_stmt(DropStmt *stmt);
extern const char * deparse_alter_enum_stmt(AlterEnumStmt *stmt);
extern const char * deparse_alter_type_stmt(AlterTableStmt *stmt);
extern const char * deparse_rename_type_stmt(RenameStmt *stmt);
extern const char * deparse_alter_type_schema_stmt(AlterObjectSchemaStmt *stmt);

extern void qualify_rename_type_stmt(RenameStmt *stmt);
extern void qualify_alter_enum_stmt(AlterEnumStmt *stmt);
extern void qualify_alter_type_stmt(AlterTableStmt *stmt);
extern void qualify_composite_type_stmt(CompositeTypeStmt *stmt);
extern void qualify_create_enum_stmt(CreateEnumStmt *stmt);
extern void qualify_alter_type_schema_stmt(AlterObjectSchemaStmt *stmt);

extern const ObjectAddress * GetObjectAddressFromParseTree(Node *parseTree, bool
														   missing_ok);

#endif /* CITUS_DEPARSER_H */
