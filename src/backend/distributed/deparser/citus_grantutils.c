#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "distributed/deparser.h"
#include "distributed/citus_ruleutils.h"

void
AppendWithGrantOption(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
}


void
AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt)
{
	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}
}


void
AppendGrantRestrictAndCascadeForRoleSpec(StringInfo buf, DropBehavior behavior, bool
										 isGrant)
{
	if (!isGrant)
	{
		if (behavior == DROP_RESTRICT)
		{
			appendStringInfo(buf, " RESTRICT");
		}
		else if (behavior == DROP_CASCADE)
		{
			appendStringInfo(buf, " CASCADE");
		}
	}
}


void
AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt *stmt)
{
	AppendGrantRestrictAndCascadeForRoleSpec(buf, stmt->behavior, stmt->is_grant);
}


void
AppendGrantedByInGrantForRoleSpec(StringInfo buf, RoleSpec *grantor, bool isGrant)
{
	if (isGrant && grantor)
	{
		appendStringInfo(buf, " GRANTED BY %s", RoleSpecString(grantor, true));
	}
}


void
AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt)
{
	AppendGrantedByInGrantForRoleSpec(buf, stmt->grantor, stmt->is_grant);
}
