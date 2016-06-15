# Makefile for tests of the Citus extension

citus_subdir = src/test/regress
citus_top_builddir = ../../..

include $(citus_top_builddir)/Makefile.global

# ensure MAJORVERSION is defined (missing in older versions)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

##
## Citus regression support
##
MULTI_INSTALLDIR=$(CURDIR)/tmp_check/install
pg_regress_multi_check = $(PERL) $(citus_abs_srcdir)/pg_regress_multi.pl --pgxsdir="$(pgxsdir)" --bindir="$(bindir)" --libdir="$(libdir)" --majorversion="$(MAJORVERSION)"
MULTI_REGRESS_OPTS = --inputdir=$(citus_abs_srcdir) $(pg_regress_locale_flags)

# XXX: Can't actually do useful testruns against install - $libdir
# etc will point to the directory configured during postgres'
# build. We could copy the installed tree around, but that's quite
# likely to be mixed with other binaries and such...
cleandir-main:
###	echo rm -rf '$(CURDIR)'/tmp_check/install
###
tempinstall-main: cleandir-main
####	mkdir -p $(MULTI_INSTALLDIR)
###	$(MAKE) DESTDIR=$(MULTI_INSTALLDIR) -C $(citus_top_builddir) install > tmp_check/install.log 2>&1

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files := $(patsubst $(citus_abs_srcdir)/input/%.source,sql/%.sql, $(wildcard $(citus_abs_srcdir)/input/*.source))
output_files := $(patsubst $(citus_abs_srcdir)/output/%.source,expected/%.out, $(wildcard $(citus_abs_srcdir)/output/*.source))

# have make check actually run all tests, but keep check-full as an
# intermediate, for muscle memory backward compatibility.
check: check-full
# check-full triggers all tests that ought to be run routinely
check-full: check-multi check-multi-task-tracker-extra check-multi-binary check-worker check-multi-fdw

# using pg_regress_multi_check unnecessarily starts up multiple nodes, which isn't needed
# for check-worker. But that's harmless besides a few cycles.
check-worker: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/worker_schedule $(EXTRA_TESTS)

check-multi: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-multi-fdw: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --load-extension=file_fdw -- \
	$(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_fdw_schedule $(EXTRA_TESTS)

check-multi-hll: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --load-extension=hll -- \
	$(MULTI_REGRESS_OPTS) $(EXTRA_TESTS) multi_create_table multi_master_protocol multi_stage_data multi_agg_approximate_distinct

check-multi-task-tracker-extra: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	--server-option=citus.task_executor_type=task-tracker \
	--server-option=citus.large_table_shard_count=1 \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_task_tracker_extra_schedule $(EXTRA_TESTS)


check-multi-binary: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	--server-option=citus.binary_worker_copy_format=on \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_binary_schedule $(EXTRA_TESTS)

clean distclean maintainer-clean:
	rm -f $(output_files) $(input_files)
	rm -rf tmp_check/
