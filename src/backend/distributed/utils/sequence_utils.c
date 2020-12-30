
#include "postgres.h"

#include "commands/sequence.h"
#include "fmgr.h"

#include "distributed/sequence_utils.h"
#include "distributed/coordinator_protocol.h"

#include "utils/builtins.h"



int UniqueId(void) {
	text *sequenceName = cstring_to_text(CITUS_UNIQUE_ID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	Datum uniqueIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	int uniqueId = DatumGetInt64(uniqueIdDatum);

	return uniqueId;
}