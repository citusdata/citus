#-------------------------------------------------------------------------
#
# Makefile for src/test/columnar_freezing
#
# Test that columnar freezing works.
#
#-------------------------------------------------------------------------

subdir = src/test/columnar_freezing
top_builddir = ../../..
include $(top_builddir)/Makefile.global

# copied from pgxs/Makefile.global to use postgres' abs build dir for pg_regress
ifeq ($(enable_tap_tests),yes)

define citus_prove_installcheck
rm -rf '$(CURDIR)'/tmp_check
$(MKDIR_P) '$(CURDIR)'/tmp_check
cd $(srcdir) && \
TESTDIR='$(CURDIR)' \
PATH="$(bindir):$$PATH" \
PGPORT='6$(DEF_PGPORT)' \
top_builddir='$(CURDIR)/$(top_builddir)' \
PG_REGRESS='$(pgxsdir)/src/test/regress/pg_regress' \
TEMP_CONFIG='$(CURDIR)'/postgresql.conf \
$(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)
endef

else
citus_prove_installcheck = @echo "TAP tests not enabled when postgres was compiled"
endif

installcheck:
	$(citus_prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
