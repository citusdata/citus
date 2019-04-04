# Makefile for the Citus extension

citus_subdir = src/backend/distributed
citus_top_builddir = ../../..

MODULE_big = citus
EXTENSION = citus
EXTVERSIONS = 5.0 5.0-1 5.0-2  \
	5.1-1 5.1-2 5.1-3 5.1-4 5.1-5 5.1-6 5.1-7 5.1-8 \
	5.2-1 5.2-2 5.2-3 5.2-4 \
	6.0-1 6.0-2 6.0-3 6.0-4 6.0-5 6.0-6 6.0-7 6.0-8 6.0-9 6.0-10 6.0-11 6.0-12 6.0-13 6.0-14 6.0-15 6.0-16 6.0-17 6.0-18 \
	6.1-1 6.1-2 6.1-3 6.1-4 6.1-5 6.1-6 6.1-7 6.1-8 6.1-9 6.1-10 6.1-11 6.1-12 6.1-13 6.1-14 6.1-15 6.1-16 6.1-17 \
	6.2-1 6.2-2 6.2-3 6.2-4 \
	7.0-1 7.0-2 7.0-3 7.0-4 7.0-5 7.0-6 7.0-7 7.0-8 7.0-9 7.0-10 7.0-11 7.0-12 7.0-13 7.0-14 7.0-15 \
	7.1-1 7.1-2 7.1-3 7.1-4 \
	7.2-1 7.2-2 7.2-3 \
	7.3-1 7.3-2 7.3-3 \
	7.4-1 7.4-2 7.4-3 \
	7.5-1 7.5-2 7.5-3 7.5-4 7.5-5 7.5-6 7.5-7 \
	8.0-1 8.0-2 8.0-3 8.0-4 8.0-5 8.0-6 8.0-7 8.0-8 8.0-9 8.0-10 8.0-11 8.0-12 8.0-13 \
	8.1-1

# All citus--*.sql files in the source directory
DATA = $(patsubst $(citus_abs_srcdir)/%.sql,%.sql,$(wildcard $(citus_abs_srcdir)/$(EXTENSION)--*--*.sql))
# Generated files for each version
DATA_built = $(foreach v,$(EXTVERSIONS),$(EXTENSION)--$(v).sql)

# directories with source files
SUBDIRS = . commands connection ddl executor master metadata planner progress relay test transaction utils worker

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
$(EXTENSION)--5.1-4.sql: $(EXTENSION)--5.1-3.sql $(EXTENSION)--5.1-3--5.1-4.sql
	cat $^ > $@
$(EXTENSION)--5.1-5.sql: $(EXTENSION)--5.1-4.sql $(EXTENSION)--5.1-4--5.1-5.sql
	cat $^ > $@
$(EXTENSION)--5.1-6.sql: $(EXTENSION)--5.1-5.sql $(EXTENSION)--5.1-5--5.1-6.sql
	cat $^ > $@
$(EXTENSION)--5.1-7.sql: $(EXTENSION)--5.1-6.sql $(EXTENSION)--5.1-6--5.1-7.sql
	cat $^ > $@
$(EXTENSION)--5.1-8.sql: $(EXTENSION)--5.1-7.sql $(EXTENSION)--5.1-7--5.1-8.sql
	cat $^ > $@
$(EXTENSION)--5.2-1.sql: $(EXTENSION)--5.1-8.sql $(EXTENSION)--5.1-8--5.2-1.sql
	cat $^ > $@
$(EXTENSION)--5.2-2.sql: $(EXTENSION)--5.2-1.sql $(EXTENSION)--5.2-1--5.2-2.sql
	cat $^ > $@
$(EXTENSION)--5.2-3.sql: $(EXTENSION)--5.2-2.sql $(EXTENSION)--5.2-2--5.2-3.sql
	cat $^ > $@
$(EXTENSION)--5.2-4.sql: $(EXTENSION)--5.2-3.sql $(EXTENSION)--5.2-3--5.2-4.sql
	cat $^ > $@
$(EXTENSION)--6.0-1.sql: $(EXTENSION)--5.2-4.sql $(EXTENSION)--5.2-4--6.0-1.sql
	cat $^ > $@
$(EXTENSION)--6.0-2.sql: $(EXTENSION)--6.0-1.sql $(EXTENSION)--6.0-1--6.0-2.sql
	cat $^ > $@
$(EXTENSION)--6.0-3.sql: $(EXTENSION)--6.0-2.sql $(EXTENSION)--6.0-2--6.0-3.sql
	cat $^ > $@
$(EXTENSION)--6.0-4.sql: $(EXTENSION)--6.0-3.sql $(EXTENSION)--6.0-3--6.0-4.sql
	cat $^ > $@
$(EXTENSION)--6.0-5.sql: $(EXTENSION)--6.0-4.sql $(EXTENSION)--6.0-4--6.0-5.sql
	cat $^ > $@
$(EXTENSION)--6.0-6.sql: $(EXTENSION)--6.0-5.sql $(EXTENSION)--6.0-5--6.0-6.sql
	cat $^ > $@
$(EXTENSION)--6.0-7.sql: $(EXTENSION)--6.0-6.sql $(EXTENSION)--6.0-6--6.0-7.sql
	cat $^ > $@
$(EXTENSION)--6.0-8.sql: $(EXTENSION)--6.0-7.sql $(EXTENSION)--6.0-7--6.0-8.sql
	cat $^ > $@
$(EXTENSION)--6.0-9.sql: $(EXTENSION)--6.0-8.sql $(EXTENSION)--6.0-8--6.0-9.sql
	cat $^ > $@
$(EXTENSION)--6.0-10.sql: $(EXTENSION)--6.0-9.sql $(EXTENSION)--6.0-9--6.0-10.sql
	cat $^ > $@
$(EXTENSION)--6.0-11.sql: $(EXTENSION)--6.0-10.sql $(EXTENSION)--6.0-10--6.0-11.sql
	cat $^ > $@
$(EXTENSION)--6.0-12.sql: $(EXTENSION)--6.0-11.sql $(EXTENSION)--6.0-11--6.0-12.sql
	cat $^ > $@
$(EXTENSION)--6.0-13.sql: $(EXTENSION)--6.0-12.sql $(EXTENSION)--6.0-12--6.0-13.sql
	cat $^ > $@
$(EXTENSION)--6.0-14.sql: $(EXTENSION)--6.0-13.sql $(EXTENSION)--6.0-13--6.0-14.sql
	cat $^ > $@
$(EXTENSION)--6.0-15.sql: $(EXTENSION)--6.0-14.sql $(EXTENSION)--6.0-14--6.0-15.sql
	cat $^ > $@
$(EXTENSION)--6.0-16.sql: $(EXTENSION)--6.0-15.sql $(EXTENSION)--6.0-15--6.0-16.sql
	cat $^ > $@
$(EXTENSION)--6.0-17.sql: $(EXTENSION)--6.0-16.sql $(EXTENSION)--6.0-16--6.0-17.sql
	cat $^ > $@
$(EXTENSION)--6.0-18.sql: $(EXTENSION)--6.0-17.sql $(EXTENSION)--6.0-17--6.0-18.sql
	cat $^ > $@
$(EXTENSION)--6.1-1.sql: $(EXTENSION)--6.0-18.sql $(EXTENSION)--6.0-18--6.1-1.sql
	cat $^ > $@
$(EXTENSION)--6.1-2.sql: $(EXTENSION)--6.1-1.sql $(EXTENSION)--6.1-1--6.1-2.sql
	cat $^ > $@
$(EXTENSION)--6.1-3.sql: $(EXTENSION)--6.1-2.sql $(EXTENSION)--6.1-2--6.1-3.sql
	cat $^ > $@
$(EXTENSION)--6.1-4.sql: $(EXTENSION)--6.1-3.sql $(EXTENSION)--6.1-3--6.1-4.sql
	cat $^ > $@
$(EXTENSION)--6.1-5.sql: $(EXTENSION)--6.1-4.sql $(EXTENSION)--6.1-4--6.1-5.sql
	cat $^ > $@
$(EXTENSION)--6.1-6.sql: $(EXTENSION)--6.1-5.sql $(EXTENSION)--6.1-5--6.1-6.sql
	cat $^ > $@
$(EXTENSION)--6.1-7.sql: $(EXTENSION)--6.1-6.sql $(EXTENSION)--6.1-6--6.1-7.sql
	cat $^ > $@
$(EXTENSION)--6.1-8.sql: $(EXTENSION)--6.1-7.sql $(EXTENSION)--6.1-7--6.1-8.sql
	cat $^ > $@
$(EXTENSION)--6.1-9.sql: $(EXTENSION)--6.1-8.sql $(EXTENSION)--6.1-8--6.1-9.sql
	cat $^ > $@
$(EXTENSION)--6.1-10.sql: $(EXTENSION)--6.1-9.sql $(EXTENSION)--6.1-9--6.1-10.sql
	cat $^ > $@
$(EXTENSION)--6.1-11.sql: $(EXTENSION)--6.1-10.sql $(EXTENSION)--6.1-10--6.1-11.sql
	cat $^ > $@
$(EXTENSION)--6.1-12.sql: $(EXTENSION)--6.1-11.sql $(EXTENSION)--6.1-11--6.1-12.sql
	cat $^ > $@
$(EXTENSION)--6.1-13.sql: $(EXTENSION)--6.1-12.sql $(EXTENSION)--6.1-12--6.1-13.sql
	cat $^ > $@
$(EXTENSION)--6.1-14.sql: $(EXTENSION)--6.1-13.sql $(EXTENSION)--6.1-13--6.1-14.sql
	cat $^ > $@
$(EXTENSION)--6.1-15.sql: $(EXTENSION)--6.1-14.sql $(EXTENSION)--6.1-14--6.1-15.sql
	cat $^ > $@
$(EXTENSION)--6.1-16.sql: $(EXTENSION)--6.1-15.sql $(EXTENSION)--6.1-15--6.1-16.sql
	cat $^ > $@
$(EXTENSION)--6.1-17.sql: $(EXTENSION)--6.1-16.sql $(EXTENSION)--6.1-16--6.1-17.sql
	cat $^ > $@
$(EXTENSION)--6.2-1.sql: $(EXTENSION)--6.1-17.sql $(EXTENSION)--6.1-17--6.2-1.sql
	cat $^ > $@
$(EXTENSION)--6.2-2.sql: $(EXTENSION)--6.2-1.sql $(EXTENSION)--6.2-1--6.2-2.sql
	cat $^ > $@
$(EXTENSION)--6.2-3.sql: $(EXTENSION)--6.2-2.sql $(EXTENSION)--6.2-2--6.2-3.sql
	cat $^ > $@
$(EXTENSION)--6.2-4.sql: $(EXTENSION)--6.2-3.sql $(EXTENSION)--6.2-3--6.2-4.sql
	cat $^ > $@
$(EXTENSION)--7.0-1.sql: $(EXTENSION)--6.2-4.sql $(EXTENSION)--6.2-4--7.0-1.sql
	cat $^ > $@
$(EXTENSION)--7.0-2.sql: $(EXTENSION)--7.0-1.sql $(EXTENSION)--7.0-1--7.0-2.sql
	cat $^ > $@
$(EXTENSION)--7.0-3.sql: $(EXTENSION)--7.0-2.sql $(EXTENSION)--7.0-2--7.0-3.sql
	cat $^ > $@
$(EXTENSION)--7.0-4.sql: $(EXTENSION)--7.0-3.sql $(EXTENSION)--7.0-3--7.0-4.sql
	cat $^ > $@
$(EXTENSION)--7.0-5.sql: $(EXTENSION)--7.0-4.sql $(EXTENSION)--7.0-4--7.0-5.sql
	cat $^ > $@
$(EXTENSION)--7.0-6.sql: $(EXTENSION)--7.0-5.sql $(EXTENSION)--7.0-5--7.0-6.sql
	cat $^ > $@
$(EXTENSION)--7.0-7.sql: $(EXTENSION)--7.0-6.sql $(EXTENSION)--7.0-6--7.0-7.sql
	cat $^ > $@
$(EXTENSION)--7.0-8.sql: $(EXTENSION)--7.0-7.sql $(EXTENSION)--7.0-7--7.0-8.sql
	cat $^ > $@
$(EXTENSION)--7.0-9.sql: $(EXTENSION)--7.0-8.sql $(EXTENSION)--7.0-8--7.0-9.sql
	cat $^ > $@
$(EXTENSION)--7.0-10.sql: $(EXTENSION)--7.0-9.sql $(EXTENSION)--7.0-9--7.0-10.sql
	cat $^ > $@
$(EXTENSION)--7.0-11.sql: $(EXTENSION)--7.0-10.sql $(EXTENSION)--7.0-10--7.0-11.sql
	cat $^ > $@
$(EXTENSION)--7.0-12.sql: $(EXTENSION)--7.0-11.sql $(EXTENSION)--7.0-11--7.0-12.sql
	cat $^ > $@
$(EXTENSION)--7.0-13.sql: $(EXTENSION)--7.0-12.sql $(EXTENSION)--7.0-12--7.0-13.sql
	cat $^ > $@
$(EXTENSION)--7.0-14.sql: $(EXTENSION)--7.0-13.sql $(EXTENSION)--7.0-13--7.0-14.sql
	cat $^ > $@
$(EXTENSION)--7.0-15.sql: $(EXTENSION)--7.0-14.sql $(EXTENSION)--7.0-14--7.0-15.sql
	cat $^ > $@
$(EXTENSION)--7.1-1.sql: $(EXTENSION)--7.0-15.sql $(EXTENSION)--7.0-15--7.1-1.sql
	cat $^ > $@
$(EXTENSION)--7.1-2.sql: $(EXTENSION)--7.1-1.sql $(EXTENSION)--7.1-1--7.1-2.sql
	cat $^ > $@
$(EXTENSION)--7.1-3.sql: $(EXTENSION)--7.1-2.sql $(EXTENSION)--7.1-2--7.1-3.sql
	cat $^ > $@
$(EXTENSION)--7.1-4.sql: $(EXTENSION)--7.1-3.sql $(EXTENSION)--7.1-3--7.1-4.sql
	cat $^ > $@
$(EXTENSION)--7.2-1.sql: $(EXTENSION)--7.1-4.sql $(EXTENSION)--7.1-4--7.2-1.sql
	cat $^ > $@
$(EXTENSION)--7.2-2.sql: $(EXTENSION)--7.2-1.sql $(EXTENSION)--7.2-1--7.2-2.sql
	cat $^ > $@
$(EXTENSION)--7.2-3.sql: $(EXTENSION)--7.2-2.sql $(EXTENSION)--7.2-2--7.2-3.sql
	cat $^ > $@
$(EXTENSION)--7.3-1.sql: $(EXTENSION)--7.2-3.sql $(EXTENSION)--7.2-3--7.3-1.sql
	cat $^ > $@
$(EXTENSION)--7.3-2.sql: $(EXTENSION)--7.3-1.sql $(EXTENSION)--7.3-1--7.3-2.sql
	cat $^ > $@
$(EXTENSION)--7.3-3.sql: $(EXTENSION)--7.3-2.sql $(EXTENSION)--7.3-2--7.3-3.sql
	cat $^ > $@
$(EXTENSION)--7.4-1.sql: $(EXTENSION)--7.3-3.sql $(EXTENSION)--7.3-3--7.4-1.sql
	cat $^ > $@
$(EXTENSION)--7.4-2.sql: $(EXTENSION)--7.4-1.sql $(EXTENSION)--7.4-1--7.4-2.sql
	cat $^ > $@
$(EXTENSION)--7.4-3.sql: $(EXTENSION)--7.4-2.sql $(EXTENSION)--7.4-2--7.4-3.sql
	cat $^ > $@
$(EXTENSION)--7.5-1.sql: $(EXTENSION)--7.4-3.sql $(EXTENSION)--7.4-3--7.5-1.sql
	cat $^ > $@
$(EXTENSION)--7.5-2.sql: $(EXTENSION)--7.5-1.sql $(EXTENSION)--7.5-1--7.5-2.sql
	cat $^ > $@
$(EXTENSION)--7.5-3.sql: $(EXTENSION)--7.5-2.sql $(EXTENSION)--7.5-2--7.5-3.sql
	cat $^ > $@
$(EXTENSION)--7.5-4.sql: $(EXTENSION)--7.5-3.sql $(EXTENSION)--7.5-3--7.5-4.sql
	cat $^ > $@
$(EXTENSION)--7.5-5.sql: $(EXTENSION)--7.5-4.sql $(EXTENSION)--7.5-4--7.5-5.sql
	cat $^ > $@
$(EXTENSION)--7.5-6.sql: $(EXTENSION)--7.5-5.sql $(EXTENSION)--7.5-5--7.5-6.sql
	cat $^ > $@
$(EXTENSION)--7.5-7.sql: $(EXTENSION)--7.5-6.sql $(EXTENSION)--7.5-6--7.5-7.sql
	cat $^ > $@
$(EXTENSION)--8.0-1.sql: $(EXTENSION)--7.5-7.sql $(EXTENSION)--7.5-7--8.0-1.sql
	cat $^ > $@
$(EXTENSION)--8.0-2.sql: $(EXTENSION)--8.0-1.sql $(EXTENSION)--8.0-1--8.0-2.sql
	cat $^ > $@
$(EXTENSION)--8.0-3.sql: $(EXTENSION)--8.0-2.sql $(EXTENSION)--8.0-2--8.0-3.sql
	cat $^ > $@
$(EXTENSION)--8.0-4.sql: $(EXTENSION)--8.0-3.sql $(EXTENSION)--8.0-3--8.0-4.sql
	cat $^ > $@
$(EXTENSION)--8.0-5.sql: $(EXTENSION)--8.0-4.sql $(EXTENSION)--8.0-4--8.0-5.sql
	cat $^ > $@
$(EXTENSION)--8.0-6.sql: $(EXTENSION)--8.0-5.sql $(EXTENSION)--8.0-5--8.0-6.sql
	cat $^ > $@
$(EXTENSION)--8.0-7.sql: $(EXTENSION)--8.0-6.sql $(EXTENSION)--8.0-6--8.0-7.sql
	cat $^ > $@
$(EXTENSION)--8.0-8.sql: $(EXTENSION)--8.0-7.sql $(EXTENSION)--8.0-7--8.0-8.sql
	cat $^ > $@
$(EXTENSION)--8.0-9.sql: $(EXTENSION)--8.0-8.sql $(EXTENSION)--8.0-8--8.0-9.sql
	cat $^ > $@
$(EXTENSION)--8.0-10.sql: $(EXTENSION)--8.0-9.sql $(EXTENSION)--8.0-9--8.0-10.sql
	cat $^ > $@
$(EXTENSION)--8.0-11.sql: $(EXTENSION)--8.0-10.sql $(EXTENSION)--8.0-10--8.0-11.sql
	cat $^ > $@
$(EXTENSION)--8.0-12.sql: $(EXTENSION)--8.0-11.sql $(EXTENSION)--8.0-11--8.0-12.sql
	cat $^ > $@
$(EXTENSION)--8.0-13.sql: $(EXTENSION)--8.0-12.sql $(EXTENSION)--8.0-12--8.0-13.sql
	cat $^ > $@
$(EXTENSION)--8.1-1.sql: $(EXTENSION)--8.0-13.sql $(EXTENSION)--8.0-13--8.1-1.sql
	cat $^ > $@

NO_PGXS = 1

SHLIB_LINK = $(libpq)

include $(citus_top_builddir)/Makefile.global

SHLIB_LINK += $(filter -lssl -lcrypto -lssleay32 -leay32, $(LIBS))

override CPPFLAGS += -I$(libpq_srcdir)
