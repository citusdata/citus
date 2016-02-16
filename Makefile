# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: extension csql

# build extension
extension:
	$(MAKE) -C src/backend/distributed/ all
install-extension: extension
	$(MAKE) -C src/backend/distributed/ install
install-headers: extension
	$(MKDIR_P) '$(includedir_server)/distributed/'
# generated headers are located in the build directory
	$(INSTALL_DATA) src/include/citus_config.h '$(includedir_server)/'
# the rest in the source tree
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/distributed/*.h '$(includedir_server)/distributed/'
clean-extension:
	$(MAKE) -C src/backend/distributed/ clean
.PHONY: extension install-extension clean-extension
# Add to generic targets
install: install-extension install-headers
clean: clean-extension

# build csql binary
csql:
	$(MAKE) -C src/bin/csql/ all
install-csql: csql
	$(MAKE) -C src/bin/csql/ install
clean-csql:
	$(MAKE) -C src/bin/csql/ clean
.PHONY: csql install-csql clean-csql
# Add to generic targets
install: install-csql
clean: clean-csql

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
