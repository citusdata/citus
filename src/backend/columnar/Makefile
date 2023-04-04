citus_subdir = src/backend/columnar
citus_top_builddir = ../../..
safestringlib_srcdir = $(citus_abs_top_srcdir)/vendor/safestringlib
SUBDIRS = . safeclib
SUBDIRS +=
ENSURE_SUBDIRS_EXIST := $(shell mkdir -p $(SUBDIRS))
OBJS += \
	$(patsubst $(citus_abs_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(citus_abs_srcdir)/$(dir)/*.c))))

MODULE_big = citus_columnar
EXTENSION = citus_columnar

template_sql_files = $(patsubst $(citus_abs_srcdir)/%,%,$(wildcard $(citus_abs_srcdir)/sql/*.sql))
template_downgrade_sql_files = $(patsubst $(citus_abs_srcdir)/sql/downgrades/%,%,$(wildcard $(citus_abs_srcdir)/sql/downgrades/*.sql))
generated_sql_files = $(patsubst %,$(citus_abs_srcdir)/build/%,$(template_sql_files))
generated_downgrade_sql_files += $(patsubst %,$(citus_abs_srcdir)/build/sql/%,$(template_downgrade_sql_files))

DATA_built = $(generated_sql_files)

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(safestringlib_srcdir)/include

include $(citus_top_builddir)/Makefile.global

SQL_DEPDIR=.deps/sql
SQL_BUILDDIR=build/sql

$(generated_sql_files): $(citus_abs_srcdir)/build/%: %
	@mkdir -p $(citus_abs_srcdir)/$(SQL_DEPDIR) $(citus_abs_srcdir)/$(SQL_BUILDDIR)
	@# -MF is used to store dependency files(.Po) in another directory for separation
	@# -MT is used to change the target of the rule emitted by dependency generation.
	@# -P is used to inhibit generation of linemarkers in the output from the preprocessor.
	@# -undef is used to not predefine any system-specific or GCC-specific macros.
	@# `man cpp` for further information
	cd $(citus_abs_srcdir) && cpp -undef -w -P -MMD -MP -MF$(SQL_DEPDIR)/$(*F).Po -MT$@ $< > $@

$(generated_downgrade_sql_files): $(citus_abs_srcdir)/build/sql/%: sql/downgrades/%
	@mkdir -p $(citus_abs_srcdir)/$(SQL_DEPDIR) $(citus_abs_srcdir)/$(SQL_BUILDDIR)
	@# -MF is used to store dependency files(.Po) in another directory for separation
	@# -MT is used to change the target of the rule emitted by dependency generation.
	@# -P is used to inhibit generation of linemarkers in the output from the preprocessor.
	@# -undef is used to not predefine any system-specific or GCC-specific macros.
	@# `man cpp` for further information
	cd $(citus_abs_srcdir) && cpp -undef -w -P -MMD -MP -MF$(SQL_DEPDIR)/$(*F).Po -MT$@ $< > $@

.PHONY: install install-downgrades install-all

cleanup-before-install:
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus_columnar.control
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/columnar--*
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus_columnar--*

install: cleanup-before-install

# install and install-downgrades should be run sequentially
install-all: install
	$(MAKE) install-downgrades

install-downgrades: $(generated_downgrade_sql_files)
	$(INSTALL_DATA) $(generated_downgrade_sql_files) '$(DESTDIR)$(datadir)/$(datamoduledir)/'

