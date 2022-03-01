/*-------------------------------------------------------------------------
 *
 * mod.h
 *
 * Type and function declarations for columnar
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MOD_H
#define MOD_H
#define COLUMNAR_LIB_NAME "citus_columnar"

/* Function declarations for extension loading and unloading */
extern void columnar_init(void);
extern void columnar_fini(void);

#endif /* MOD_H */
