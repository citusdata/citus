# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..

MODULE_big = citus
EXTENSION = citus

template_sql_files = $(patsubst $(citus_abs_srcdir)/%,%,$(wildcard $(citus_abs_srcdir)/sql/*.sql))
generated_sql_files = $(patsubst %,build/%,$(template_sql_files))
# All citus--*.sql files that are used to upgrade between versions
DATA_built = $(generated_sql_files)

# directories with source files
SUBDIRS = . commands connection ddl executor master metadata planner progress relay test transaction utils worker

# That patsubst rule searches all directories listed in SUBDIRS for .c
# files, and adds the corresponding .o files to OBJS
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

# be explicit about the default target
all:

NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citus_top_builddir)/Makefile.global

SHLIB_LINK += $(filter -lssl -lcrypto -lssleay32 -leay32, $(LIBS))

override CPPFLAGS += -I$(libpq_srcdir)

SQL_DEPDIR=.deps/sql
SQL_BUILDDIR=build/sql

$(SQL_DEPDIR) $(SQL_BUILDDIR):
	mkdir -p $(citus_abs_srcdir)/$@

$(generated_sql_files): build/%: % $(SQL_DEPDIR) $(SQL_BUILDDIR)
	cd $(citus_abs_srcdir) && cpp -undef -w -P -MMD -MP -MF $(SQL_DEPDIR)/$(*F).Po -MT $@ $< > $@

SQL_Po_files := $(wildcard $(SQL_DEPDIR)/*.Po)
ifneq (,$(SQL_Po_files))
include $(SQL_Po_files)
endif
