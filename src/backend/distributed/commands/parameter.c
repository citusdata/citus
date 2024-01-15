#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "distributed/metadata_sync.h"
#include "distributed/deparser.h"
#include "distributed/commands.h"


List *
PostprocessGrantParameterStmt(Node *node, const char *queryString)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	char *command = DeparseTreeNode(node);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) command,
								ENABLE_DDL_PROPAGATION);

	return NontransactionalNodeDDLTaskList(REMOTE_NODES, commands);
}
