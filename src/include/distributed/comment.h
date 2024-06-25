/*-------------------------------------------------------------------------
 *
 * comment.h
 *    Declarations for comment related operations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_COMMENT_H
#define CITUS_COMMENT_H

#include "postgres.h"

#include "nodes/parsenodes.h"


extern const char *ObjectTypeNames[];


extern List * GetCommentPropagationCommands(Oid classOid, Oid oid, char *objectName,
											ObjectType objectType);
extern List * GetCommentPropagationCommandsX(Oid classOid, Oid oid, char *objectName,
											 ObjectType objectType, char *qualifier,
											 int32 subid);
extern List * CommentObjectAddress(Node *node, bool missing_ok, bool isPostprocess);

# endif /* CITUS_COMMENT_H */
