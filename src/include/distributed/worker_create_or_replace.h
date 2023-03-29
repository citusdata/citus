/*-------------------------------------------------------------------------
 *
 * worker_create_or_replace.h
 *	  Header for handling CREATE OR REPLACE of objects,
 *	  even if postgres lacks CREATE OR REPLACE for those objects
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_CREATE_OR_REPLACE_H
#define WORKER_CREATE_OR_REPLACE_H

#include "catalog/objectaddress.h"

#define CREATE_OR_REPLACE_COMMAND "SELECT worker_create_or_replace_object(%s);"

extern char * WrapCreateOrReplace(const char *sql);
extern char * WrapCreateOrReplaceList(List *sqls);
extern char * GenerateBackupNameForCollision(const ObjectAddress *address);
extern DropStmt * CreateDropStmt(const ObjectAddress *address);
extern RenameStmt * CreateRenameStatement(const ObjectAddress *address, char *newName);

#endif /* WORKER_CREATE_OR_REPLACE_H */
