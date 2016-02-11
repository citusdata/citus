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

citusdb_subdir = src/bin/csql
citusdb_top_builddir = ../../..

PROGRAM = csql

PGFILEDESC = "csql - the CitusDB interactive terminal"
PGAPPICON=win32

OBJS =command.o common.o help.o input.o stringutils.o mainloop.o copy.o \
      copy_options.o stage.o \
      startup.o prompt.o variables.o large_obj.o print.o describe.o \
      tab-complete.o mbprint.o dumputils.o keywords.o kwlookup.o \
      sql_help.o \
      $(WIN32RES)

PG_LIBS = $(libpq)

include $(citusdb_top_builddir)/Makefile.global

override CPPFLAGS += -I$(libpq_srcdir) -I$(top_srcdir)/src/bin/csql

# psqlscan is compiled as part of mainloop
mainloop.o: psqlscan.c
psqlscan.c: FLEXFLAGS = -Cfe -p -p
psqlscan.c: FLEX_NO_BACKUP=yes

clean: csql-clean
csql-clean:
	rm -f csql$(X) $(OBJS) psqlscan.c lex.backup
