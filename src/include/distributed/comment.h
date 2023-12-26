/*-------------------------------------------------------------------------
 *
 * comment.h
 *    Declarations for comment related operations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMMENT_H
#define COMMENT_H

#include "postgres.h"

#include "nodes/parsenodes.h"

typedef struct CommentStmtType
{
	ObjectType objectType;
	char *objectName;
} CommentStmtType;


extern List * GetCommentPropagationCommands(Oid oid, char *objectName, ObjectType
											objectType);
extern List * CommentObjectAddress(Node *node, bool missing_ok, bool isPostprocess);

# endif /* COMMENT_H */
