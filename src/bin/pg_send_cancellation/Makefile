citus_top_builddir = ../../..

PROGRAM = pg_send_cancellation
PGFILEDESC = "pg_send_cancellation sends a custom cancellation message"
OBJS = $(citus_abs_srcdir)/src/bin/pg_send_cancellation/pg_send_cancellation.o
PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL = $(libpq_pgport)
PG_LDFLAGS += $(LDFLAGS)

include $(citus_top_builddir)/Makefile.global

# We reuse all the Citus flags (incl. security flags), but we are building a program not a shared library
# We sometimes build Citus with a newer version of gcc than Postgres was built
# with and this breaks LTO (link-time optimization). Even if disabling it can
# have some perf impact this is ok because pg_send_cancellation is only used
# for tests anyway.
override CFLAGS := $(filter-out -shared, $(CFLAGS)) -fno-lto

# Filter out unneeded dependencies
override LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses -lpam, $(LIBS))

clean: clean-pg_send_cancellation
clean-pg_send_cancellation:
	rm -f $(PROGRAM) $(OBJS)
