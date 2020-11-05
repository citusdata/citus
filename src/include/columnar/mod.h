/*-------------------------------------------------------------------------
 *
 * mod.h
 *
 * Type and function declarations for CStore
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MOD_H
#define MOD_H

/* Function declarations for extension loading and unloading */
extern void columnar_init(void);
extern void columnar_fini(void);

#endif /* MOD_H */
