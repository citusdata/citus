/*-------------------------------------------------------------------------
 *
 * worker_create_or_replace.h
 *	  Header for handling CREATE OR REPLACE of objects,
 *	  even if postgres lacks CREATE OR REPLACE for those objects
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_CREATE_OR_REPLACE_H
#define WORKER_CREATE_OR_REPLACE_H

#define CREATE_OR_REPLACE_COMMAND "SELECT worker_create_or_replace_object(%s);"

extern char * WrapCreateOrReplace(const char *sql);

#endif /* WORKER_CREATE_OR_REPLACE_H */
