# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..

MODULE_big = citus
EXTENSION = citus
EXTVERSION = 5.0
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
# Install scripts in build and/or source directory, but only once if those are the same
SCRIPTS = $(sort $(wildcard $(citus_top_builddir)/src/bin/scripts/* $(citus_top_srcdir)/src/bin/scripts/*))

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
