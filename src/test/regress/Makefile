# Makefile for tests of the CitusDB extension

citusdb_subdir = src/test/regress
citusdb_top_builddir = ../../..

include $(citusdb_top_builddir)/Makefile.global

# ensure MAJORVERSION is defined (missing in older versions)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

##
## CitusDB regression support
##
MULTI_INSTALLDIR=$(CURDIR)/tmp_check/install
pg_regress_multi_check = $(PERL) $(citusdb_abs_srcdir)/pg_regress_multi.pl --pgxsdir="$(pgxsdir)" --bindir="$(bindir)" --libdir="$(libdir)" --majorversion="$(MAJORVERSION)"
MULTI_REGRESS_OPTS = --inputdir=$(citusdb_abs_srcdir) $(pg_regress_locale_flags)

# XXX: Can't actually do useful testruns against install - $libdir
# etc will point to the directory configured during postgres'
# build. We could copy the installed tree around, but that's quite
# likely to be mixed with other binaries and such...
cleandir-main:
###	echo rm -rf '$(CURDIR)'/tmp_check/install
###
tempinstall-main: cleandir-main
####	mkdir -p $(MULTI_INSTALLDIR)
###	$(MAKE) DESTDIR=$(MULTI_INSTALLDIR) -C $(citusdb_top_builddir) install > tmp_check/install.log 2>&1

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files := $(patsubst $(citusdb_abs_srcdir)/input/%.source,sql/%.sql, $(wildcard $(citusdb_abs_srcdir)/input/*.source))
output_files := $(patsubst $(citusdb_abs_srcdir)/output/%.source,expected/%.out, $(wildcard $(citusdb_abs_srcdir)/output/*.source))

# have make check actually run all tests, but keep check-full as an
# intermediate, for muscle memory backward compatibility.
check: check-full
# check-full triggers all tests that ought to be run routinely
check-full: check-multi check-multi-task-tracker check-multi-binary check-worker check-multi-fdw

# using pg_regress_multi_check unnecessarily starts up multiple nodes, which isn't needed
# for check-worker. But that's harmless besides a few cycles.
check-worker: all
	$(pg_regress_multi_check) --load-extension=citusdb \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citusdb_abs_srcdir)/worker_schedule $(EXTRA_TESTS)

check-multi: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citusdb \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citusdb_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-multi-fdw: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citusdb --load-extension=file_fdw -- \
	$(MULTI_REGRESS_OPTS) --schedule=$(citusdb_abs_srcdir)/multi_fdw_schedule $(EXTRA_TESTS)

check-multi-hll: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citusdb --load-extension=hll -- \
	$(MULTI_REGRESS_OPTS) $(EXTRA_TESTS) multi_create_table multi_master_protocol multi_stage_data multi_agg_approximate_distinct

check-multi-task-tracker: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citusdb \
	--server-option=citusdb.task_executor_type=task-tracker \
	--server-option=citusdb.task_tracker_delay=50ms \
	--server-option=citusdb.large_table_shard_count=1 \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citusdb_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-multi-binary: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citusdb \
	--server-option=citusdb.binary_worker_copy_format=on \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citusdb_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

clean distclean maintainer-clean:
	rm -f $(output_files) $(input_files)
	rm -rf tmp_check/
