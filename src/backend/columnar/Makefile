citus_subdir = src/backend/columnar
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
SUBDIRS = . safeclib
SUBDIRS +=
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

# Symlinks are not copied over to the build directory if a separete build
# directory is used during configure (such as on CI)
ENSURE_SUBDIRS_EXIST := $(shell mkdir -p $(SUBDIRS))

MODULE_big = columnar


PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
citus_top_builddir = ../../..
include $(citus_top_builddir)/Makefile.global
endif
