citus_subdir = src/backend/columnar
citus_top_builddir = ../../..
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
SUBDIRS = . safeclib
SUBDIRS +=
ENSURE_SUBDIRS_EXIST := $(shell mkdir -p $(SUBDIRS))
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

MODULE_big = citus_columnar
EXTENSION = citus_columnar

DATA = sql/columnar--9.5-1--10.0-1.sql

#DATA = $(wildcard sql/*--*.sql) 
#		$(wildcard sql/downgrades/*--*.sql)

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
include $(citus_top_builddir)/Makefile.global

.PHONY: install-all
install-all: install
endif
