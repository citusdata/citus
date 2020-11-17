# cstore_fdw/Makefile
#
# Copyright (c) 2016 Citus Data, Inc.
#

MODULE_big = cstore_fdw

VER := $(lastword $(shell pg_config --version))
VER_WORDS = $(subst ., ,$(VER))
MVER = $(firstword $(VER_WORDS))

# error for versions earlier than 10 so that lex comparison will work
ifneq ($(shell printf '%02d' $(MVER)),$(MVER))
$(error version $(VER) not supported)
endif

# lexicographic comparison of version number
ifeq ($(lastword $(sort 12 $(MVER))),$(MVER))
	USE_TABLEAM = yes
	USE_FDW = yes
else ifeq ($(lastword $(sort 11 $(MVER))),$(MVER))
	USE_TABLEAM = no
	USE_FDW = yes
else
$(error version $(VER) is not supported)
endif

PG_CFLAGS = -std=c11 -Wshadow -Werror
OBJS = cstore.o cstore_writer.o cstore_reader.o \
       cstore_compression.o mod.o cstore_metadata_tables.o

EXTENSION = cstore_fdw
DATA = cstore_fdw--1.7.sql cstore_fdw--1.6--1.7.sql  cstore_fdw--1.5--1.6.sql cstore_fdw--1.4--1.5.sql \
	   cstore_fdw--1.3--1.4.sql cstore_fdw--1.2--1.3.sql cstore_fdw--1.1--1.2.sql \
	   cstore_fdw--1.0--1.1.sql cstore_fdw--1.7--1.8.sql

REGRESS = extension_create
ISOLATION = create
EXTRA_CLEAN = sql/fdw_block_filtering.sql sql/fdw_create.sql sql/fdw_data_types.sql sql/fdw_load.sql \
              sql/fdw_copyto.sql expected/fdw_block_filtering.out expected/fdw_create.out \
              expected/fdw_data_types.out expected/fdw_load.out expected/fdw_copyto.out \
              sql/am_block_filtering.sql sql/am_create.sql sql/am_data_types.sql sql/am_load.sql \
              sql/am_copyto.sql expected/am_block_filtering.out \
              expected/am_data_types.out expected/am_load.out expected/am_copyto.out

ifeq ($(USE_FDW),yes)
	PG_CFLAGS += -DUSE_FDW
	OBJS += cstore_fdw.o
	REGRESS += fdw_create fdw_load fdw_query fdw_analyze fdw_data_types \
		   fdw_functions fdw_block_filtering fdw_drop fdw_insert \
		   fdw_copyto fdw_alter fdw_rollback fdw_truncate fdw_clean
endif

ifeq ($(USE_TABLEAM),yes)
	PG_CFLAGS += -DUSE_TABLEAM
	OBJS += cstore_tableam.o cstore_customscan.o
	REGRESS += am_create am_load am_query am_analyze am_data_types am_functions \
	           am_drop am_insert am_copyto am_alter am_rollback am_truncate am_vacuum am_clean \
	           am_block_filtering am_join am_trigger am_tableoptions
	ISOLATION += am_write_concurrency am_vacuum_vs_insert
endif

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PG_CPPFLAGS += -I/usr/local/include
endif

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.3/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# command for getting postgres source directory is taken from citus/configure.in
POSTGRES_SRCDIR=$(shell grep ^abs_top_srcdir $(shell dirname $(shell $(PG_CONFIG) --pgxs))/../Makefile.global|cut -d ' ' -f3-)
PGXS_ISOLATION_TESTER=$(top_builddir)/src/test/isolation/pg_isolation_regress

# If postgres installation doesn't include pg_isolation_regress, try using the
# one in postgres source directory.
ifeq (,$(wildcard $(PGXS_ISOLATION_TESTER)))
	pg_isolation_regress_installcheck = \
		$(POSTGRES_SRCDIR)/src/test/isolation/pg_isolation_regress \
		--inputdir=$(srcdir) $(EXTRA_REGRESS_OPTS)
else
	pg_isolation_regress_installcheck = \
		$(PGXS_ISOLATION_TESTER) \
		--inputdir=$(srcdir) $(EXTRA_REGRESS_OPTS)
endif

installcheck:

reindent:
	citus_indent .
