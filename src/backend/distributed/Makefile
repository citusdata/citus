# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..

MODULE_big = citus
EXTENSION = citus
EXTVERSIONS = 5.0 5.0-1 5.0-2  \
	5.1-1 5.1-2 5.1-3

# All citus--*.sql files in the source directory
DATA = $(patsubst $(citus_abs_srcdir)/%.sql,%.sql,$(wildcard $(citus_abs_srcdir)/$(EXTENSION)--*--*.sql))
# Generated files for each version
DATA_built = $(foreach v,$(EXTVERSIONS),$(EXTENSION)--$(v).sql)
SCRIPTS = ../../bin/scripts/copy_to_distributed_table

# directories with source files
SUBDIRS = . commands executor master planner relay test utils worker

# That patsubst rule searches all directories listed in SUBDIRS for .c
# files, and adds the corresponding .o files to OBJS
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

# be explicit about the default target
all:

# generate each version's file installation file by concatenating
# previous upgrade scripts
$(EXTENSION)--5.0.sql: $(EXTENSION).sql
	cat $^ > $@
$(EXTENSION)--5.0-1.sql: $(EXTENSION)--5.0.sql $(EXTENSION)--5.0--5.0-1.sql
	cat $^ > $@
$(EXTENSION)--5.0-2.sql: $(EXTENSION)--5.0-1.sql $(EXTENSION)--5.0-1--5.0-2.sql
	cat $^ > $@
$(EXTENSION)--5.1-1.sql: $(EXTENSION)--5.0-2.sql $(EXTENSION)--5.0-2--5.1-1.sql
	cat $^ > $@
$(EXTENSION)--5.1-2.sql: $(EXTENSION)--5.1-1.sql $(EXTENSION)--5.1-1--5.1-2.sql
	cat $^ > $@
$(EXTENSION)--5.1-3.sql: $(EXTENSION)--5.1-2.sql $(EXTENSION)--5.1-2--5.1-3.sql
	cat $^ > $@
	
NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citus_top_builddir)/Makefile.global

override CPPFLAGS += -I$(libpq_srcdir)
