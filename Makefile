# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
	$(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: extension pg_send_cancellation


# build columnar only
columnar:
	$(MAKE) -C src/backend/columnar all
# build extension
extension: $(citus_top_builddir)/src/include/citus_version.h columnar
	$(MAKE) -C src/backend/distributed/ all
install-columnar: columnar
	$(MAKE) -C src/backend/columnar install
install-extension: extension install-columnar
	$(MAKE) -C src/backend/distributed/ install
install-headers: extension
	$(MKDIR_P) '$(DESTDIR)$(includedir_server)/distributed/'
# generated headers are located in the build directory
	$(INSTALL_DATA) $(citus_top_builddir)/src/include/citus_version.h '$(DESTDIR)$(includedir_server)/'
# the rest in the source tree
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/distributed/*.h '$(DESTDIR)$(includedir_server)/distributed/'

clean-extension:
	$(MAKE) -C src/backend/distributed/ clean
	$(MAKE) -C src/backend/columnar/ clean
clean-full:
	$(MAKE) -C src/backend/distributed/ clean-full
.PHONY: extension install-extension clean-extension clean-full

install-downgrades:
	$(MAKE) -C src/backend/distributed/ install-downgrades
install-all: install-headers install-pg_send_cancellation
	$(MAKE) -C src/backend/columnar/ install-all
	$(MAKE) -C src/backend/distributed/ install-all

# build citus_send_cancellation binary
pg_send_cancellation:
	$(MAKE) -C src/bin/pg_send_cancellation/ all
install-pg_send_cancellation: pg_send_cancellation
	$(MAKE) -C src/bin/pg_send_cancellation/ install
clean-pg_send_cancellation:
	$(MAKE) -C src/bin/pg_send_cancellation/ clean
.PHONY: pg_send_cancellation install-pg_send_cancellation clean-pg_send_cancellation

# Add to generic targets
install: install-extension install-headers install-pg_send_cancellation
clean: clean-extension clean-pg_send_cancellation

# apply or check style
reindent:
	${citus_abs_top_srcdir}/ci/fix_style.sh
check-style:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style

# depend on install-all so that downgrade scripts are installed as well
check: all install-all
	$(MAKE) -C src/test/regress check-full

.PHONY: all check clean install install-downgrades install-all
