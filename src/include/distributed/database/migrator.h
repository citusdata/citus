/*-------------------------------------------------------------------------
 *
 * pg_migrator.h
 *	  definition of pg_migrator data types
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_MIGRATOR_H
#define PG_MIGRATOR_H


#define PG_MIGRATOR_EXTENSION_NAME "pg_migrator"


/* settings that controls whether to show commands */
extern bool ShowRemoteCommands;
extern bool ShowLocalCommands;


#endif
