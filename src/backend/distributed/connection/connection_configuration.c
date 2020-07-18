/*-------------------------------------------------------------------------
 *
 * connection_configuration.c
 *   Functions for controlling configuration of Citus connections
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_manager.h"

#include "postmaster/postmaster.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"

/* stores the string representation of our node connection GUC */
char *NodeConninfo = "";

/* represents a list of libpq parameter settings */
typedef struct ConnParamsInfo
{
	char **keywords; /* libpq keywords */
	char **values; /* desired values for above */
	Size size; /* current used size of arrays */
	Size maxSize; /* maximum allocated size of arrays (similar to e.g. StringInfo) */
} ConnParamsInfo;

/*
 * Stores parsed global libpq parameter settings. static because all access
 * is encapsulated in the other public functions in this file.
 */
static ConnParamsInfo ConnParams;

/* helper functions for processing connection info */
static Size CalculateMaxSize(void);
static int uri_prefix_length(const char *connstr);

/*
 * InitConnParms initializes the ConnParams field to point to enough memory to
 * store settings for every valid libpq value, though these regions are set to
 * zeros from the outset and the size appropriately also set to zero.
 *
 * This function must be called before others in this file, though calling it
 * after use of the initialized ConnParams structure will result in any
 * populated parameter settings being lost.
 */
void
InitConnParams()
{
	Size maxSize = CalculateMaxSize();
	ConnParamsInfo connParams = {
		.keywords = malloc(maxSize * sizeof(char *)),
		.values = malloc(maxSize * sizeof(char *)),
		.size = 0,
		.maxSize = maxSize
	};

	memset(connParams.keywords, 0, maxSize * sizeof(char *));
	memset(connParams.values, 0, maxSize * sizeof(char *));

	ConnParams = connParams;
}


/*
 * ResetConnParams frees all strings in the keywords and parameters arrays,
 * sets their elements to null, and resets the ConnParamsSize to zero before
 * adding back any hardcoded global connection settings (at present, only the
 * fallback_application_name of 'citus').
 */
void
ResetConnParams()
{
	for (Size paramIdx = 0; paramIdx < ConnParams.size; paramIdx++)
	{
		free((void *) ConnParams.keywords[paramIdx]);
		free((void *) ConnParams.values[paramIdx]);

		ConnParams.keywords[paramIdx] = ConnParams.values[paramIdx] = NULL;
	}

	ConnParams.size = 0;

	InvalidateConnParamsHashEntries();

	AddConnParam("fallback_application_name", CITUS_APPLICATION_NAME);
}


/*
 * AddConnParam adds a parameter setting to the global libpq settings according
 * to the provided keyword and value.
 */
void
AddConnParam(const char *keyword, const char *value)
{
	if (ConnParams.size + 1 >= ConnParams.maxSize)
	{
		/* hopefully this error is only seen by developers */
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("ConnParams arrays bound check failed")));
	}

	ConnParams.keywords[ConnParams.size] = strdup(keyword);
	ConnParams.values[ConnParams.size] = strdup(value);
	ConnParams.size++;

	ConnParams.keywords[ConnParams.size] = ConnParams.values[ConnParams.size] = NULL;
}


/*
 * CheckConninfo is a building block to help implement check constraints and
 * other check hooks against libpq-like conninfo strings. In particular, the
 * provided conninfo must:
 *
 *   - Not use a uri-prefix such as postgres:// (it must be only keys and values)
 *   - Parse using PQconninfoParse
 *   - Only set keywords contained in the provided allowedConninfoKeywords
 *
 * This function returns true if all of the above are satisfied, otherwise it
 * returns false. If the provided errmsg pointer is not NULL, it will be set
 * to an appropriate message if the check fails.
 *
 * The provided allowedConninfoKeywords must be sorted in a manner usable by bsearch,
 * though this is only validated during assert-enabled builds.
 */
bool
CheckConninfo(const char *conninfo, const char **allowedConninfoKeywords,
			  Size allowedConninfoKeywordsLength, char **errorMsg)
{
	PQconninfoOption *option = NULL;
	char *errorMsgString = NULL;

	/*
	 * If the user doesn't need a message, just overwrite errmsg with a stack
	 * variable so we can always safely write to it.
	 */
	if (errorMsg == NULL)
	{
		errorMsg = &errorMsgString;
	}

	/* sure, it can be null */
	if (conninfo == NULL)
	{
		return true;
	}

	/* the libpq prefix form is more complex than we need; ban it */
	if (uri_prefix_length(conninfo) != 0)
	{
		*errorMsg = "Citus connection info strings must be in "
					"'k1=v1 k2=v2 [...] kn=vn' format";

		return false;
	}

	/* this should at least parse */
	PQconninfoOption *optionArray = PQconninfoParse(conninfo, NULL);
	if (optionArray == NULL)
	{
		*errorMsg = "Provided string is not a valid libpq connection info string";

		return false;
	}

#ifdef USE_ASSERT_CHECKING

	/* verify that the allowedConninfoKeywords is in ascending order */
	for (Size keywordIdx = 1; keywordIdx < allowedConninfoKeywordsLength; keywordIdx++)
	{
		const char *prev = allowedConninfoKeywords[keywordIdx - 1];
		const char *curr = allowedConninfoKeywords[keywordIdx];

		AssertArg(strcmp(prev, curr) < 0);
	}
#endif

	for (option = optionArray; option->keyword != NULL; option++)
	{
		if (option->val == NULL || option->val[0] == '\0')
		{
			continue;
		}

		void *matchingKeyword = SafeBsearch(&option->keyword, allowedConninfoKeywords,
											allowedConninfoKeywordsLength, sizeof(char *),
											pg_qsort_strcmp);
		if (matchingKeyword == NULL)
		{
			/* the allowedConninfoKeywords lacks this keyword; error out! */
			StringInfoData msgString;
			initStringInfo(&msgString);

			appendStringInfo(&msgString, "Prohibited conninfo keyword detected: %s",
							 option->keyword);

			*errorMsg = msgString.data;

			break;
		}
	}

	PQconninfoFree(optionArray);

	/* if error message is set we found an invalid keyword */
	return (*errorMsg == NULL);
}


/*
 * GetConnParams uses the provided key to determine libpq parameters needed to
 * establish a connection using that key. The keywords and values are placed in
 * the like-named out parameters. All parameter strings are allocated in the
 * context provided by the caller, to save the caller needing to copy strings
 * into an appropriate context later.
 */
void
GetConnParams(ConnectionHashKey *key, char ***keywords, char ***values,
			  Index *runtimeParamStart, MemoryContext context)
{
	/*
	 * make space for the port as a string: sign, 10 digits, NUL. We keep it on the stack
	 * till we can later copy it to the right context. By having the declaration here
	 * already we can add a pointer to the runtimeValues.
	 */
	char nodePortString[12] = "";

	/*
	 * This function has three sections:
	 *   - Initialize the keywords and values (to be copied later) of global parameters
	 *   - Append user/host-specific parameters calculated from the given key
	 *   - (Enterprise-only) append user/host-specific authentication params
	 *
	 * The global parameters have already been assigned from a GUC, so begin by
	 * calculating the key-specific parameters (basically just the fields of
	 * the key and the active database encoding).
	 *
	 * We allocate everything in the provided context so as to facilitate using
	 * pfree on all runtime parameters when connections using these entries are
	 * invalidated during config reloads.
	 */
	const char *runtimeKeywords[] = {
		"host",
		"port",
		"dbname",
		"user",
		"client_encoding"
	};
	const char *runtimeValues[] = {
		key->hostname,
		nodePortString,
		key->database,
		key->user,
		GetDatabaseEncodingName()
	};

	/*
	 * remember where global/GUC params end and runtime ones start, all entries after this
	 * point should be allocated in context and will be freed upon
	 * FreeConnParamsHashEntryFields
	 */
	*runtimeParamStart = ConnParams.size;

	/*
	 * Declare local params for readability;
	 *
	 * assignment is done directly to not loose the pointers if any of the later
	 * allocations cause an error. FreeConnParamsHashEntryFields knows about the
	 * possibility of half initialized keywords or values and correctly reclaims them when
	 * the cache is reused.
	 *
	 * Need to zero enough space for all possible libpq parameters.
	 */
	char **connKeywords = *keywords = MemoryContextAllocZero(context, ConnParams.maxSize *
															 sizeof(char *));
	char **connValues = *values = MemoryContextAllocZero(context, ConnParams.maxSize *
														 sizeof(char *));

	/* auth keywords will begin after global and runtime ones are appended */
	Index authParamsIdx = ConnParams.size + lengthof(runtimeKeywords);

	if (ConnParams.size + lengthof(runtimeKeywords) >= ConnParams.maxSize)
	{
		/* hopefully this error is only seen by developers */
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("too many connParams entries")));
	}

	pg_ltoa(key->port, nodePortString); /* populate node port string with port */

	/* first step: copy global parameters to beginning of array */
	for (Size paramIndex = 0; paramIndex < ConnParams.size; paramIndex++)
	{
		/* copy the keyword&value pointers to the new array */
		connKeywords[paramIndex] = ConnParams.keywords[paramIndex];
		connValues[paramIndex] = ConnParams.values[paramIndex];
	}

	/* second step: begin after global params and copy runtime params into our context */
	for (Index runtimeParamIndex = 0;
		 runtimeParamIndex < lengthof(runtimeKeywords);
		 runtimeParamIndex++)
	{
		/* copy the keyword & value into our context and append to the new array */
		connKeywords[ConnParams.size + runtimeParamIndex] =
			MemoryContextStrdup(context, runtimeKeywords[runtimeParamIndex]);
		connValues[ConnParams.size + runtimeParamIndex] =
			MemoryContextStrdup(context, runtimeValues[runtimeParamIndex]);
	}

	/* final step: add terminal NULL, required by libpq */
	connKeywords[authParamsIdx] = connValues[authParamsIdx] = NULL;
}


/*
 * GetConnParam finds the keyword in the configured connection parameters and returns its
 * value.
 */
const char *
GetConnParam(const char *keyword)
{
	for (Size i = 0; i < ConnParams.size; i++)
	{
		if (strcmp(keyword, ConnParams.keywords[i]) == 0)
		{
			return ConnParams.values[i];
		}
	}

	return NULL;
}


/*
 * CalculateMaxSize simply counts the number of elements returned by
 * PQconnDefaults, including the final NULL. This helps us know how space would
 * be used if a connection utilizes every known libpq parameter.
 */
static Size
CalculateMaxSize()
{
	PQconninfoOption *defaults = PQconndefaults();
	Size maxSize = 0;

	for (PQconninfoOption *option = defaults;
		 option->keyword != NULL;
		 option++, maxSize++)
	{
		/* do nothing, we're just counting the elements */
	}

	PQconninfoFree(defaults);

	/* we've counted elements but libpq needs a final NULL, so add one */
	maxSize++;

	return maxSize;
}


/* *INDENT-OFF* */

/*
 * Checks if connection string starts with either of the valid URI prefix
 * designators.
 *
 * Returns the URI prefix length, 0 if the string doesn't contain a URI prefix.
 *
 * This implementation (mostly) taken from libpq/fe-connect.c.
 */
static int
uri_prefix_length(const char *connstr)
{
	const char uri_designator[] = "postgresql://";
	const char short_uri_designator[] = "postgres://";

	if (strncmp(connstr, uri_designator,
				sizeof(uri_designator) - 1) == 0)
		return sizeof(uri_designator) - 1;

	if (strncmp(connstr, short_uri_designator,
				sizeof(short_uri_designator) - 1) == 0)
		return sizeof(short_uri_designator) - 1;

	return 0;
}

/* *INDENT-ON* */
