/*-------------------------------------------------------------------------
 *
 * qualify_function_stmt.c
 *	  Functions specialized in fully qualifying all function statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying function statements consists of adding the schema name
 *	  to the subject of the function and types as well as any other branch of
 *    the parsetree.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"

/* TODO: implement this and add some comments here */
void
QualifyAlterFunctionStmt(AlterFunctionStmt *stmt)
{ }


/* TODO: implement this and add some comments here */
void
QualifyRenameFunctionStmt(RenameStmt *stmt)
{ }


/* TODO: implement this and add some comments here */
void
QualifyAlterFunctionSchemaStmt(AlterObjectSchemaStmt *stmt)
{ }


/* TODO: implement this and add some comments here */
void
QualifyAlterFunctionOwnerStmt(AlterOwnerStmt *stmt)
{ }


/* TODO: implement this and add some comments here */
void
QualifyAlterFunctionDependsStmt(AlterObjectDependsStmt *stmt)
{ }
