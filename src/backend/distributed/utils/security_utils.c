
#include "postgres.h"
#include "distributed/security_utils.h"
#include "distributed/metadata_cache.h"

static Oid savedUserId = InvalidOid;
static int savedSecurityContext = 0;

void PushCitusSecurityContext(void) {
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);
    
}

void PopCitusSecurityContext(void) {
    SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}