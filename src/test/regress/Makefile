# Makefile for tests of the Citus extension

citus_subdir = src/test/regress
citus_top_builddir = ../../..

include $(citus_top_builddir)/Makefile.global

# ensure MAJORVERSION is defined (missing in older versions)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

# Add ./bin to $PATH so we use our custom diff instead of the default diff tool.
# We do this to be able to mask shard Ids, placement Ids, node ports, etc.
MAKEFILE_DIR := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
export PATH := $(MAKEFILE_DIR)/bin:$(PATH)
export PG_REGRESS_DIFF_OPTS = -dU10 -w

##
## Citus regression support
##
MULTI_INSTALLDIR=$(CURDIR)/tmp_check/install
pg_regress_multi_check = $(PERL) $(citus_abs_srcdir)/pg_regress_multi.pl --pgxsdir="$(pgxsdir)" --bindir="$(bindir)" --libdir="$(libdir)" --majorversion="$(MAJORVERSION)" --postgres-builddir="$(postgres_abs_builddir)"  --postgres-srcdir="$(postgres_abs_srcdir)"
MULTI_REGRESS_OPTS = --inputdir=$(citus_abs_srcdir) $(pg_regress_locale_flags) --launcher="$(citus_abs_srcdir)/log_test_times"

pg_upgrade_check = $(citus_abs_srcdir)/upgrade/pg_upgrade_test.py
citus_upgrade_check = $(citus_abs_srcdir)/upgrade/citus_upgrade_test.py

template_mx_files = $(shell find $(citus_abs_srcdir)/specs/ -name 'base*mx.spec')
generated_mx_files = $(patsubst $(citus_abs_srcdir)/specs/base_isolation%,$(citus_abs_srcdir)/specs/isolation%,$(template_mx_files))
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
check-full: check-multi check-multi-mx check-multi-task-tracker-extra check-worker check-follower-cluster check-failure

# this can be used to print a value of variable
# ex: make print-generated_mx_files
print-%  : ; @echo $* = $($*)

$(generated_mx_files): $(citus_abs_srcdir)/specs/%: $(citus_abs_srcdir)/specs/base_%
	echo $@
	cd $(citus_abs_srcdir) && cpp -undef -w -P -MMD -MP -MT $@ $< > $@

# using pg_regress_multi_check unnecessarily starts up multiple nodes, which isn't needed
# for check-worker. But that's harmless besides a few cycles.
check-worker: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/worker_schedule $(EXTRA_TESTS)

# check-base only sets up a testing environment so you can specify all your tests using EXTRA_TESTS
check-base: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/base_schedule $(EXTRA_TESTS)

check-base-non-adaptive: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	--server-option=citus.task_executor_type=real-time \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/base_schedule $(EXTRA_TESTS)

# check-minimal only sets up the cluster
check-minimal: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/minimal_schedule $(EXTRA_TESTS)

check-base-vg: all
	$(pg_regress_multi_check) --load-extension=citus \
	--valgrind --pg_ctl-timeout=360 --connection-timeout=500000 --valgrind-path=valgrind --valgrind-log-file=$(VALGRIND_LOG_FILE) \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/base_schedule $(EXTRA_TESTS)

check-base-mx: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/mx_base_schedule $(EXTRA_TESTS)

check-empty: all
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) $(EXTRA_TESTS)

check-multi: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-multi-non-adaptive: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	--server-option=citus.task_executor_type=real-time \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-failure-non-adaptive: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --mitmproxy \
	--server-option=citus.task_executor_type=real-time \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/failure_schedule $(EXTRA_TESTS)

check-failure-non-adaptive-base: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --mitmproxy \
	--server-option=citus.task_executor_type=real-time \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/failure_base_schedule $(EXTRA_TESTS)

check-isolation-non-adaptive: all tempinstall-main $(generated_mx_files)
	$(pg_regress_multi_check) --load-extension=citus --isolationtester \
	--server-option=citus.task_executor_type=real-time \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/isolation_schedule $(EXTRA_TESTS)

check-multi-vg: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --valgrind \
	--pg_ctl-timeout=360 --connection-timeout=500000 --valgrind-path=valgrind --valgrind-log-file=$(VALGRIND_LOG_FILE) \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_schedule $(EXTRA_TESTS)

check-isolation: all tempinstall-main $(generated_mx_files)
	$(pg_regress_multi_check) --load-extension=citus --isolationtester \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/isolation_schedule $(EXTRA_TESTS)

check-isolation-base: all tempinstall-main $(generated_mx_files)
	$(pg_regress_multi_check) --load-extension=citus --isolationtester \
	-- $(MULTI_REGRESS_OPTS) $(EXTRA_TESTS)

check-vanilla: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus --vanillatest

check-multi-mx: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_mx_schedule $(EXTRA_TESTS)

check-multi-task-tracker-extra: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	--server-option=citus.task_executor_type=task-tracker \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_task_tracker_extra_schedule $(EXTRA_TESTS)

check-follower-cluster: all
	$(pg_regress_multi_check) --load-extension=citus --follower-cluster \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/multi_follower_schedule $(EXTRA_TESTS)

check-failure: all
	$(pg_regress_multi_check) --load-extension=citus --mitmproxy \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/failure_schedule $(EXTRA_TESTS)

check-failure-base: all
	$(pg_regress_multi_check) --load-extension=citus --mitmproxy \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/failure_base_schedule $(EXTRA_TESTS)

check-pg-upgrade:
	$(pg_upgrade_check) --old-bindir=$(old-bindir) --new-bindir=$(new-bindir) --pgxsdir=$(pgxsdir)

check-citus-upgrade:
	$(citus_upgrade_check) \
		--bindir=$(bindir) \
		--pgxsdir=$(pgxsdir) \
		--citus-pre-tar=$(citus-pre-tar) \
		--citus-post-tar=$(citus-post-tar)

check-citus-upgrade-mixed:
	$(citus_upgrade_check) \
		--bindir=$(bindir) \
		--pgxsdir=$(pgxsdir) \
		--citus-pre-tar=$(citus-pre-tar) \
		--citus-post-tar=$(citus-post-tar) \
		--mixed

check-citus-upgrade-local:
	$(citus_upgrade_check) \
		--bindir=$(bindir) \
		--pgxsdir=$(pgxsdir) \
		--citus-old-version=$(citus-old-version)	

check-citus-upgrade-mixed-local:
	$(citus_upgrade_check) \
		--bindir=$(bindir) \
		--pgxsdir=$(pgxsdir) \
		--citus-old-version=$(citus-old-version) \
		--mixed			

clean distclean maintainer-clean:
	rm -f $(output_files) $(input_files)
	rm -rf tmp_check/
