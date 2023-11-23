#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

/*
 * Append the 'WITH GRANT OPTION' clause to the given buffer if the given
 * statement is a 'GRANT' statement and the grant option is specified.
 */
void
AppendWithGrantOption(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
}


/*
 * Append the 'GRANT OPTION FOR' clause to the given buffer if the given
 * statement is a 'REVOKE' statement and the grant option is specified.
 */
void
AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt)
{
	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}
}


/*
 * Append the 'RESTRICT' or 'CASCADE' clause to the given buffer if the given
 * statement is a 'REVOKE' statement and the behavior is specified.
 */
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


/*
 * Append the 'RESTRICT' or 'CASCADE' clause to the given buffer using 'GrantStmt',
 * if the given statement is a 'REVOKE' statement and the behavior is specified.
 */
void
AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt *stmt)
{
	AppendGrantRestrictAndCascadeForRoleSpec(buf, stmt->behavior, stmt->is_grant);
}


/*
 * Append the 'GRANTED BY' clause to the given buffer if the given statement is a
 * 'GRANT' statement and the grantor is specified.
 */
void
AppendGrantedByInGrantForRoleSpec(StringInfo buf, RoleSpec *grantor, bool isGrant)
{
	if (isGrant && grantor)
	{
		appendStringInfo(buf, " GRANTED BY %s", RoleSpecString(grantor, true));
	}
}


/*
 * Append the 'GRANTED BY' clause to the given buffer using 'GrantStmt',
 * if the given statement is a 'GRANT' statement and the grantor is specified.
 */
void
AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt)
{
	AppendGrantedByInGrantForRoleSpec(buf, stmt->grantor, stmt->is_grant);
}


void
AppendGrantSharedPrefix(StringInfo buf, GrantStmt *stmt)
{
	appendStringInfo(buf, "%s ", stmt->is_grant ? "GRANT" : "REVOKE");
	AppendGrantOptionFor(buf, stmt);
	AppendGrantPrivileges(buf, stmt);
}


void
AppendGrantSharedSuffix(StringInfo buf, GrantStmt *stmt)
{
	AppendGrantGrantees(buf, stmt);
	AppendWithGrantOption(buf, stmt);
	AppendGrantRestrictAndCascade(buf, stmt);
	AppendGrantedByInGrant(buf, stmt);
	appendStringInfo(buf, ";");
}
