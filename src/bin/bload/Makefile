#-------------------------------------------------------------------------
#
# Makefile for src/bin/bload
#
# Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/bload/Makefile
#
#-------------------------------------------------------------------------

citus_subdir = src/bin/bload
citus_top_builddir = ../../..

PROGRAM = bload

PGFILEDESC = "bload - the zeromq client for bulkload"

OBJS = bload.o

PG_LIBS = $(libpq)

override CFLAGS += -lzmq

include $(citus_top_builddir)/Makefile.global

clean: bload-clean
bload-clean:
	rm -f bload$(X) $(OBJS)
