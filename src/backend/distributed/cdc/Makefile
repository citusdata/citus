ifndef DECODER
	DECODER = pgoutput
endif

MODULE_big = citus_$(DECODER)

.PHONY: clean-full install install-all

OBJS += cdc_decoder.o cdc_decoder_utils.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override CFLAGS += -std=gnu99 -Wno-declaration-after-statement

install: install-cdc

clean: clean-cdc

install-cdc:
	mkdir -p '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders'
	$(INSTALL_SHLIB) citus_$(DECODER).so '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'

clean-cdc:
	rm -f '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'
