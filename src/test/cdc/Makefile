#-------------------------------------------------------------------------
#
# Makefile for src/test/cdc
#
# Test that CDC publication works correctly.
#
#-------------------------------------------------------------------------

subdir = src/test/cdc
top_builddir = ../../..
include $(top_builddir)/Makefile.global

pg_version = $(shell $(PG_CONFIG) --version 2>/dev/null)
pg_whole_version = $(shell echo "$(pg_version)"| sed -e 's/^PostgreSQL \([0-9]*\)\(\.[0-9]*\)\{0,1\}\(.*\)/\1\2/')
pg_major_version = $(shell echo "$(pg_whole_version)"| sed -e 's/^\([0-9]\{2\}\)\(.*\)/\1/')
export pg_major_version

test_path = t/*.pl


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
$(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),$(test_path))
endef

else
citus_prove_installcheck = @echo "TAP tests not enabled when postgres was compiled"
endif

installcheck:
	$(citus_prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
