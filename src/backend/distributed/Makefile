# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
safestringlib_builddir = $(citus_top_builddir)/vendor/safestringlib/build
safestringlib_a = $(safestringlib_builddir)/libsafestring_static.a
safestringlib_sources = $(wildcard $(safestringlib_srcdir)/safeclib/*)

MODULE_big = citus
EXTENSION = citus

template_sql_files = $(patsubst $(citus_abs_srcdir)/%,%,$(wildcard $(citus_abs_srcdir)/sql/*.sql))
generated_sql_files = $(patsubst %,$(citus_abs_srcdir)/build/%,$(template_sql_files))
# All citus--*.sql files that are used to upgrade between versions
DATA_built = $(generated_sql_files)

# directories with source files
SUBDIRS = . commands connection ddl deparser executor master metadata planner progress relay test transaction utils worker

# That patsubst rule searches all directories listed in SUBDIRS for .c
# files, and adds the corresponding .o files to OBJS
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

# be explicit about the default target
all:

NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citus_top_builddir)/Makefile.global

# make sure citus_version.o is recompiled whenever any change is made to the binary or any
# other artifact being installed to reflect the correct gitref for every build
CITUS_VERSION_INVALIDATE := $(filter-out utils/citus_version.o,$(OBJS))
CITUS_VERSION_INVALIDATE += $(generated_sql_files)
ifneq ($(wildcard $(citus_top_builddir)/.git/.*),)
        CITUS_VERSION_INVALIDATE += $(citus_top_builddir)/.git/index
endif
utils/citus_version.o: $(CITUS_VERSION_INVALIDATE)

SHLIB_LINK += $(filter -lssl -lcrypto -lssleay32 -leay32, $(LIBS))

override LDFLAGS += $(safestringlib_a)
override CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include

SQL_DEPDIR=.deps/sql
SQL_BUILDDIR=build/sql

$(safestringlib_a): $(safestringlib_sources)
	rm -rf $(safestringlib_builddir)
	mkdir -p $(safestringlib_builddir)
	cd $(safestringlib_builddir) && cmake $(safestringlib_srcdir) && make -j5

citus.so: $(safestringlib_a)

$(generated_sql_files): $(citus_abs_srcdir)/build/%: %
	@mkdir -p $(citus_abs_srcdir)/$(SQL_DEPDIR) $(citus_abs_srcdir)/$(SQL_BUILDDIR)
	cd $(citus_abs_srcdir) && cpp -undef -w -P -MMD -MP -MF$(SQL_DEPDIR)/$(*F).Po -MT$@ $< > $@

SQL_Po_files := $(wildcard $(SQL_DEPDIR)/*.Po)
ifneq (,$(SQL_Po_files))
include $(SQL_Po_files)
endif

.PHONY: check-sql-snapshots

check-sql-snapshots:
	bash -c '\
	set -eu -o pipefail; \
	for f in sql/udfs/*; do \
		latest_snapshot=$$(ls $$f | { grep -v latest.sql || true; } | sort -V | tail -n 1); \
		diff -u $$f/latest.sql $$f/$$latest_snapshot; \
	done'

cleanup-before-install:
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus*

install: cleanup-before-install
