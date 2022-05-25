#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/replication/pgoutput
#
# IDENTIFICATION
#    src/backend/replication/pgoutput
#
#-------------------------------------------------------------------------
citus_top_builddir = ../../../..
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
SUBDIRS = . safeclib
SUBDIRS +=
ENSURE_SUBDIRS_EXIST := $(shell mkdir -p $(SUBDIRS))
#OBJS += \
        $(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))
OBJS += pgoutput.o

MODULE_big = decoding_plugin_for_shard_split

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include

include $(citus_top_builddir)/Makefile.global

.PHONY: install-all
install-all: install
