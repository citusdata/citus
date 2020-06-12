/*-------------------------------------------------------------------------
 *
 * tdigest_extension.c
 *    Helper functions to get access to tdigest specific data.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_TDIGEST_EXTENSION_H
#define CITUS_TDIGEST_EXTENSION_H

/* tdigest related functions */
extern Oid TDigestExtensionSchema(void);
extern Oid TDigestExtensionTypeOid(void);
extern Oid TDigestExtensionAggTDigest1(void);
extern Oid TDigestExtensionAggTDigest2(void);
extern Oid TDigestExtensionAggTDigestPercentile2(void);
extern Oid TDigestExtensionAggTDigestPercentile2a(void);
extern Oid TDigestExtensionAggTDigestPercentile3(void);
extern Oid TDigestExtensionAggTDigestPercentile3a(void);
extern Oid TDigestExtensionAggTDigestPercentileOf2(void);
extern Oid TDigestExtensionAggTDigestPercentileOf2a(void);
extern Oid TDigestExtensionAggTDigestPercentileOf3(void);
extern Oid TDigestExtensionAggTDigestPercentileOf3a(void);

#endif /* CITUS_TDIGEST_EXTENSION_H */
