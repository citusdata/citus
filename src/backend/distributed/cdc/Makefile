ifndef DECODER
	DECODER = pgoutput
endif

MODULE_big = citus_$(DECODER)
citus_top_builddir = ../../../..
citus_subdir = src/backend/distributed/cdc

.PHONY: clean-full install install-all

OBJS += cdc_decoder.o cdc_decoder_utils.o


include $(citus_top_builddir)/Makefile.global

CLAGS += -I$(citus_top_builddir)/src/include/distributed

install: install-cdc

clean: clean-cdc

install-cdc:
	mkdir -p '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders'
	$(INSTALL_SHLIB) citus_$(DECODER).so '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'

clean-cdc:
	rm -f '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'
