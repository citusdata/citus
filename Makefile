# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: extension

# build extension
extension: $(citus_abs_srcdir)/src/include/citus_version.h
	$(MAKE) -C src/backend/distributed/ all
install-extension: extension
	$(MAKE) -C src/backend/distributed/ install
install-headers: extension
	$(MKDIR_P) '$(DESTDIR)$(includedir_server)/distributed/'
# generated headers are located in the build directory
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/citus_config.h '$(DESTDIR)$(includedir_server)/'
# the rest in the source tree
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/distributed/*.h '$(DESTDIR)$(includedir_server)/distributed/'
clean-extension:
	$(MAKE) -C src/backend/distributed/ clean
.PHONY: extension install-extension clean-extension
# Add to generic targets
install: install-extension install-headers
clean: clean-extension

# apply or check style
reindent:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet
check-style:
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style

# depend on install for now
check: all install
	$(MAKE) -C src/test/regress check-full

.PHONY: all check install clean
