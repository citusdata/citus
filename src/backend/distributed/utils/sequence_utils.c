
#include "postgres.h"

#include "commands/sequence.h"
#include "fmgr.h"

#include "distributed/sequence_utils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/security_utils.h"

#include "utils/builtins.h"



int UniqueId(void) {
	
	text *sequenceName = cstring_to_text(CITUS_UNIQUE_ID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	PushCitusSecurityContext();
	Datum uniqueIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);
	PopCitusSecurityContext();
	int uniqueId = DatumGetInt64(uniqueIdDatum);
	
	return uniqueId;
}