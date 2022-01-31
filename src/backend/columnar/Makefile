citus_subdir = src/backend/columnar
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
SUBDIRS = . safeclib
SUBDIRS +=
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))


MODULE_big = columnar


PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/backend/columnar
top_builddir = ../../..
include $(top_builddir)/Makefile.global
endif
