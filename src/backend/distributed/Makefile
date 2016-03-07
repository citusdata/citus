# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..

MODULE_big = citus
EXTENSION = citus
EXTVERSION = 5.0
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
SCRIPTS = ../../bin/scripts/copy_to_distributed_table

# directories with source files
SUBDIRS = . commands executor master planner relay test utils worker

# That patsubst rule searches all directories listed in SUBDIRS for .c
# files, and adds the corresponding .o files to OBJS
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(wildcard $(citus_abs_srcdir)/$(dir)/*.c)))

# be explicit about the default target
all:

# define build process for latest install file
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cat $^ > $@

NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citus_top_builddir)/Makefile.global

override CPPFLAGS += -I$(libpq_srcdir)
