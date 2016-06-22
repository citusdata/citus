#-------------------------------------------------------------------------
#
# Makefile for src/bin/csql
#
# Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/csql/Makefile
#
#-------------------------------------------------------------------------

citus_subdir = src/bin/csql
citus_top_builddir = ../../..

PROGRAM = csql

PGFILEDESC = "csql - the Citus interactive terminal"
PGAPPICON=win32

OBJS =command.o common.o help.o input.o stringutils.o mainloop.o copy.o \
      copy_options.o stage.o \
      startup.o prompt.o variables.o large_obj.o print.o describe.o \
      tab-complete.o mbprint.o dumputils.o keywords.o kwlookup.o \
      sql_help.o \
      $(WIN32RES)

PG_LIBS = $(libpq)

include $(citus_top_builddir)/Makefile.global

# ensure client includes occur before server
client_includes := $(shell $(PG_CONFIG) --includedir)/internal

override CPPFLAGS := -I$(client_includes) -I$(libpq_srcdir) -I$(citus_abs_top_srcdir)/src/bin/csql $(CPPFLAGS)

# psqlscan is compiled as part of mainloop
mainloop.o: psqlscan.c
psqlscan.c: FLEXFLAGS = -Cfe -p -p

psqlscan.c: psqlscan.l
	$(FLEX) $(FLEXFLAGS) -o'$@' $<

clean: csql-clean
csql-clean:
	rm -f csql$(X) $(OBJS) psqlscan.c lex.backup
