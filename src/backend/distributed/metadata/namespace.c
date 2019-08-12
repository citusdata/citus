
#include "postgres.h"

#include "nodes/primnodes.h"
#include "nodes/value.h"

#include "distributed/metadata/namespace.h"

/*
 * makeNameListFromRangeVar makes a namelist from a RangeVar. Its behaviour should be the
 * exact opposite of postgres' makeRangeVarFromNameList.
 */
List *
makeNameListFromRangeVar(const RangeVar *var)
{
	if (var->catalogname != NULL)
	{
		Assert(var->schemaname != NULL);
		Assert(var->relname != NULL);
		return list_make3(makeString(var->catalogname),
						  makeString(var->schemaname),
						  makeString(var->relname));
	}
	else if (var->schemaname != NULL)
	{
		Assert(var->relname != NULL);
		return list_make2(makeString(var->schemaname),
						  makeString(var->relname));
	}
	else
	{
		Assert(var->relname != NULL);
		return list_make1(makeString(var->relname));
	}
}
