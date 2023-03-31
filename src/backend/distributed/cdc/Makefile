ifneq ($(DECODER),)
    DECODER_NAME=$(DECODER)
else
	DECODER_NAME = pgoutput
	DECODER = pgoutput
endif

citus_subdir = src/backend/distributed/cdc
citus_top_builddir = ../../../..

MODULE_big = citus_${DECODER_NAME}
EXTENSION = citus_${DECODER_NAME}

.PHONY: clean-full install install-all

OBJS = cdc_decoder.o cdc_decoder_utils.o
SHLIB_LINK = $(libpq)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
INCDIR := $(shell $(PG_CONFIG) --includedir)
include $(PGXS)

override CFLAGS += -std=gnu99 -Wno-declaration-after-statement

clean:
	rm -f *.o *.so

cleanup-before-install:
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus_$(DECODER).control
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus-$(DECODER).so
	rm -f $(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so

install: cleanup-before-install
	mkdir -p '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders'
	$(INSTALL_SHLIB) citus_$(DECODER).so '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'
	$(INSTALL_DATA) citus_$(DECODER).control '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).control'

