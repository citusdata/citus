#SUBDIRS = . safeclib
#OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
MODULE_big = columnar

OBJS = safeclib/mem_primitives_lib.o \
       safeclib/memcpy_s.o \
	   safeclib/safe_str_constraint.o \
	   safeclib/safe_mem_constraint.o \
	   safeclib/ignore_handler_s.o \
	   safeclib/abort_handler_s.o \
       columnar.o \
       columnar_tableam.o \
	   columnar_storage.o \
	   columnar_compression.o \
	   columnar_customscan.o \
	   columnar_debug.o \
	   columnar_metadata.o \
	   columnar_reader.o \
	   columnar_writer.o \
	   mod.o \
	   write_state_management.o 

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include 
EXTENSION = columnar

DATA = $(wildcard sql/*--*.sql)
REGRESS = columnar

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/backend/columnar
top_builddir = ../../..
include $(top_builddir)/Makefile.global
endif
