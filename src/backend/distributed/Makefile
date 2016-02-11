# Makefile for the CitusDB extension

citusdb_subdir = src/backend/distributed
citusdb_top_builddir = ../../..

MODULE_big = citusdb
EXTENSION = citusdb
EXTVERSION = 5.0
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
SCRIPTS = $(wildcard $(citusdb_top_builddir)/src/bin/scripts/*)

# directories with source files
SUBDIRS = . commands executor master planner relay test utils worker

# That patsubst rule searches all directories listed in SUBDIRS for .c
# files, and adds the corresponding .o files to OBJS
OBJS += \
	$(patsubst $(citusdb_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(wildcard $(citusdb_abs_srcdir)/$(dir)/*.c)))

# define build process for latest install file
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cat $^ > $@

# be explicit about the default target
all:

NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citusdb_top_builddir)/Makefile.global

override CPPFLAGS += -I$(libpq_srcdir)
