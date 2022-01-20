MODULE_big = columnar
OBJS = mem_primitives_lib.o \
       memcpy_s.o \
	   safe_str_constraint.o \
	   safe_mem_constraint.o \
	   ignore_handler_s.o \
	   abort_handler_s.o \
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

safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
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
