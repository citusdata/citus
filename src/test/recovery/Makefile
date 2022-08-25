#-------------------------------------------------------------------------
#
# Makefile for src/test/recovery
#
# Losely based on the makefile found in postgres' src/test/recovery.
# We need to define our own invocation of prove to pass the correct path
# to pg_regress and include citus in the shared preload libraries.
#
#-------------------------------------------------------------------------

subdir = src/test/recovery
top_builddir = ../../..
include $(top_builddir)/Makefile.global

# In PG15, Perl test modules have been moved to a new namespace
# new() and get_new_node() methods have been unified to 1 method: new()
# Relevant PG commits 201a76183e2056c2217129e12d68c25ec9c559c8
#                     b3b4d8e68ae83f432f43f035c7eb481ef93e1583
pg_version = $(shell $(PG_CONFIG) --version 2>/dev/null)
pg_whole_version = $(shell echo "$(pg_version)"| sed -e 's/^PostgreSQL \([0-9]*\)\(\.[0-9]*\)\{0,1\}\(.*\)/\1\2/')
pg_major_version = $(shell echo "$(pg_whole_version)"| sed -e 's/^\([0-9]\{2\}\)\(.*\)/\1/')

# for now, we only have a single test file
# due to the above explanation, we ended up separating the test paths for
# different versions. If you need to add new test files, be careful to add both versions
ifeq ($(pg_major_version),13)
	test_path = t_pg13_pg14/*.pl
else ifeq ($(pg_major_version),14)
	test_path = t_pg13_pg14/*.pl
else
    test_path = t/*.pl
endif

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
