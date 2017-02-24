/*-------------------------------------------------------------------------
 *
 * bload.h
 *    Definitions of const variables used in bulkload copy for
 *    distributed tables.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BLOAD_H
#define BLOAD_H

#define BatchSize 1024 /* size of a zeromq message in bytes */
#define MaxRecordSize 256 /* size of max acceptable record in bytes */

#endif
