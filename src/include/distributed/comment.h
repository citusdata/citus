#include "postgres.h"

#include "nodes/parsenodes.h"

typedef struct CommentStmtType
{
	ObjectType objectType;
	char *objectName;
} CommentStmtType;


extern List * GetCommentPropagationCommands(Oid oid, char *objectName, ObjectType
											objectType);
