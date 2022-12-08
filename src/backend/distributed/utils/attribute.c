//
// Created by Nils Dijk on 02/12/2022.
//

#include "postgres.h"

#include "executor/execdesc.h"
#include "utils/builtins.h"

#include "distributed/utils/attribute.h"

#include <time.h>

static void AttributeMetricsIfApplicable(void);

ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

#define ATTRIBUTE_PREFIX "/* attributeTo: "

/* TODO maybe needs to be a stack */
const char *attributeToTenant = NULL;
clock_t attributeToTenantStart = { 0 };

void
AttributeQueryIfAnnotated(const char *query_string)
{
	if (strncmp(ATTRIBUTE_PREFIX, query_string, strlen(ATTRIBUTE_PREFIX)) == 0)
	{
		/* TODO create a function to safely parse the tenant identifier from the query comment */
		/* query is attributed to a tenant */
		char *tenantId = (char*)query_string + strlen(ATTRIBUTE_PREFIX);
		char *tenantEnd = tenantId;
		while (true && tenantEnd[0] != '\0')
		{
			if (tenantEnd[0] == ' ' && tenantEnd[1] == '*' && tenantEnd[2] == '/')
			{
				break;
			}

			tenantEnd++;
		}

		/* hack to get a clean copy of the tenant id string */
		char tenantEndTmp = *tenantEnd;
		*tenantEnd = '\0';
		tenantId = pstrdup(tenantId);
		*tenantEnd = tenantEndTmp;

		ereport(NOTICE, (errmsg("attributing query to tenant: %s", quote_literal_cstr(tenantId))));

		attributeToTenant = tenantId;
	}
	else
	{
		Assert(attributeToTenant == NULL);
	}

	attributeToTenantStart = clock();
}


void
CitusAttributeToEnd(QueryDesc *queryDesc)
{
	/*
	 * At the end of the Executor is the last moment we have to attribute the previous
	 * attribution to a tenant, if applicable
	 */
	AttributeMetricsIfApplicable();

	/* now call in to the previously installed hook, or the standard implementation */
	if (prev_ExecutorEnd)
	{
		prev_ExecutorEnd(queryDesc);
	}
	else
	{
		standard_ExecutorEnd(queryDesc);
	}
}


static void
AttributeMetricsIfApplicable()
{
	if (attributeToTenant)
	{
		clock_t end = { 0 };
		double cpu_time_used = 0;

		end = clock();
		cpu_time_used = ((double) (end - attributeToTenantStart)) / CLOCKS_PER_SEC;

		ereport(NOTICE, (errmsg("attribute cpu counter (%f) to tenant: %s", cpu_time_used,
								attributeToTenant)));
	}
	attributeToTenant = NULL;
}
