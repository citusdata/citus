/*-------------------------------------------------------------------------
 *
 * citus_read.c
 *	  Citus version of postgres' read.c, using a different state variable for
 *	  citus_pg_strtok.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * NOTES
 *    Unfortunately we have to copy this file as the state variable for
 *    pg_strtok is not externally accessible. That prevents creating a a
 *    version of stringToNode() that calls CitusNodeRead() instead of
 *    nodeRead().  Luckily these functions seldomly change.
 *
 *    Keep as closely aligned with the upstream version as possible.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "nodes/pg_list.h"
#include "nodes/readfuncs.h"
#include "distributed/citus_nodefuncs.h"
#include "nodes/value.h"


/* Static state for citus_pg_strtok */
static char *citus_pg_strtok_ptr = NULL;


/*
 * CitusStringToNode -
 *	  returns a Node with a given legal ASCII representation
 */
void *
CitusStringToNode(char *str)
{
	char	   *save_strtok;
	void	   *retval;

	/*
	 * We save and restore the pre-existing state of citus_pg_strtok. This makes the
	 * world safe for re-entrant invocation of stringToNode, without incurring
	 * a lot of notational overhead by having to pass the next-character
	 * pointer around through all the readfuncs.c code.
	 */
	save_strtok = citus_pg_strtok_ptr;

	citus_pg_strtok_ptr = str;		/* point citus_pg_strtok at the string to read */

	retval = CitusNodeRead(NULL, 0); /* do the reading */

	citus_pg_strtok_ptr = save_strtok;

	return retval;
}

/*
 * citus_pg_strtok is a copy of postgres' pg_strtok routine, referencing
 * citus_pg_strtok_ptr instead of pg_strtok_ptr as state.
*/
char *
citus_pg_strtok(int *length)
{
	char	   *local_str;		/* working pointer to string */
	char	   *ret_str;		/* start of token to return */

	local_str = citus_pg_strtok_ptr;

	while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
		local_str++;

	if (*local_str == '\0')
	{
		*length = 0;
		citus_pg_strtok_ptr = local_str;
		return NULL;			/* no more tokens */
	}

	/*
	 * Now pointing at start of next token.
	 */
	ret_str = local_str;

	if (*local_str == '(' || *local_str == ')' ||
		*local_str == '{' || *local_str == '}')
	{
		/* special 1-character token */
		local_str++;
	}
	else
	{
		/* Normal token, possibly containing backslashes */
		while (*local_str != '\0' &&
			   *local_str != ' ' && *local_str != '\n' &&
			   *local_str != '\t' &&
			   *local_str != '(' && *local_str != ')' &&
			   *local_str != '{' && *local_str != '}')
		{
			if (*local_str == '\\' && local_str[1] != '\0')
				local_str += 2;
			else
				local_str++;
		}
	}

	*length = local_str - ret_str;

	/* Recognize special case for "empty" token */
	if (*length == 2 && ret_str[0] == '<' && ret_str[1] == '>')
		*length = 0;

	citus_pg_strtok_ptr = local_str;

	return ret_str;
}

#define RIGHT_PAREN (1000000 + 1)
#define LEFT_PAREN	(1000000 + 2)
#define LEFT_BRACE	(1000000 + 3)
#define OTHER_TOKEN (1000000 + 4)

/*
 * nodeTokenType -
 *	  returns the type of the node token contained in token.
 *	  It returns one of the following valid NodeTags:
 *		T_Integer, T_Float, T_String, T_BitString
 *	  and some of its own:
 *		RIGHT_PAREN, LEFT_PAREN, LEFT_BRACE, OTHER_TOKEN
 *
 *	  Assumption: the ascii representation is legal
 */
static NodeTag
nodeTokenType(char *token, int length)
{
	NodeTag		retval;
	char	   *numptr;
	int			numlen;

	/*
	 * Check if the token is a number
	 */
	numptr = token;
	numlen = length;
	if (*numptr == '+' || *numptr == '-')
		numptr++, numlen--;
	if ((numlen > 0 && isdigit((unsigned char) *numptr)) ||
		(numlen > 1 && *numptr == '.' && isdigit((unsigned char) numptr[1])))
	{
		/*
		 * Yes.  Figure out whether it is integral or float; this requires
		 * both a syntax check and a range check. strtol() can do both for us.
		 * We know the token will end at a character that strtol will stop at,
		 * so we do not need to modify the string.
		 */
		long		val;
		char	   *endptr;

		errno = 0;
		val = strtol(token, &endptr, 10);
		(void) val;				/* avoid compiler warning if unused */
		if (endptr != token + length || errno == ERANGE
#ifdef HAVE_LONG_INT_64
		/* if long > 32 bits, check for overflow of int4 */
			|| val != (long) ((int32) val)
#endif
			)
			return T_Float;
		return T_Integer;
	}

	/*
	 * these three cases do not need length checks, since citus_pg_strtok() will
	 * always treat them as single-byte tokens
	 */
	else if (*token == '(')
		retval = LEFT_PAREN;
	else if (*token == ')')
		retval = RIGHT_PAREN;
	else if (*token == '{')
		retval = LEFT_BRACE;
	else if (*token == '\"' && length > 1 && token[length - 1] == '\"')
		retval = T_String;
	else if (*token == 'b')
		retval = T_BitString;
	else
		retval = OTHER_TOKEN;
	return retval;
}


/*
 * CitusNodeRead is an adapted copy of postgres' nodeRead routine, using
 * citus_pg_strtok_ptr instead of pg_strtok_ptr.
 */
void *
CitusNodeRead(char *token, int tok_len)
{
	Node	   *result;
	NodeTag		type;

	if (token == NULL)			/* need to read a token? */
	{
		token = citus_pg_strtok(&tok_len);

		if (token == NULL)		/* end of input */
			return NULL;
	}

	type = nodeTokenType(token, tok_len);

	switch ((int) type)
	{
		case LEFT_BRACE:
			result = CitusParseNodeString();
			token = citus_pg_strtok(&tok_len);
			if (token == NULL || token[0] != '}')
				elog(ERROR, "did not find '}' at end of input node");
			break;
		case LEFT_PAREN:
			{
				List	   *l = NIL;

				/*----------
				 * Could be an integer list:	(i int int ...)
				 * or an OID list:				(o int int ...)
				 * or a list of nodes/values:	(node node ...)
				 *----------
				 */
				token = citus_pg_strtok(&tok_len);
				if (token == NULL)
					elog(ERROR, "unterminated List structure");
				if (tok_len == 1 && token[0] == 'i')
				{
					/* List of integers */
					for (;;)
					{
						int			val;
						char	   *endptr;

						token = citus_pg_strtok(&tok_len);
						if (token == NULL)
							elog(ERROR, "unterminated List structure");
						if (token[0] == ')')
							break;
						val = (int) strtol(token, &endptr, 10);
						if (endptr != token + tok_len)
							elog(ERROR, "unrecognized integer: \"%.*s\"",
								 tok_len, token);
						l = lappend_int(l, val);
					}
				}
				else if (tok_len == 1 && token[0] == 'o')
				{
					/* List of OIDs */
					for (;;)
					{
						Oid			val;
						char	   *endptr;

						token = citus_pg_strtok(&tok_len);
						if (token == NULL)
							elog(ERROR, "unterminated List structure");
						if (token[0] == ')')
							break;
						val = (Oid) strtoul(token, &endptr, 10);
						if (endptr != token + tok_len)
							elog(ERROR, "unrecognized OID: \"%.*s\"",
								 tok_len, token);
						l = lappend_oid(l, val);
					}
				}
				else
				{
					/* List of other node types */
					for (;;)
					{
						/* We have already scanned next token... */
						if (token[0] == ')')
							break;
						l = lappend(l, CitusNodeRead(token, tok_len));
						token = citus_pg_strtok(&tok_len);
						if (token == NULL)
							elog(ERROR, "unterminated List structure");
					}
				}
				result = (Node *) l;
				break;
			}
		case RIGHT_PAREN:
			elog(ERROR, "unexpected right parenthesis");
			result = NULL;		/* keep compiler happy */
			break;
		case OTHER_TOKEN:
			if (tok_len == 0)
			{
				/* must be "<>" --- represents a null pointer */
				result = NULL;
			}
			else
			{
				elog(ERROR, "unrecognized token: \"%.*s\"", tok_len, token);
				result = NULL;	/* keep compiler happy */
			}
			break;
		case T_Integer:

			/*
			 * we know that the token terminates on a char atol will stop at
			 */
			result = (Node *) makeInteger(atol(token));
			break;
		case T_Float:
			{
				char	   *fval = (char *) palloc(tok_len + 1);

				memcpy(fval, token, tok_len);
				fval[tok_len] = '\0';
				result = (Node *) makeFloat(fval);
			}
			break;
		case T_String:
			/* need to remove leading and trailing quotes, and backslashes */
			result = (Node *) makeString(debackslash(token + 1, tok_len - 2));
			break;
		case T_BitString:
			{
				char	   *val = palloc(tok_len);

				/* skip leading 'b' */
				memcpy(val, token + 1, tok_len - 1);
				val[tok_len - 1] = '\0';
				result = (Node *) makeBitString(val);
				break;
			}
		default:
			elog(ERROR, "unrecognized node type: %d", (int) type);
			result = NULL;		/* keep compiler happy */
			break;
	}

	return (void *) result;
}
