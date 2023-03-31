ifndef DECODER
	DECODER = pgoutput
endif

MODULE_big = citus_$(DECODER)
citus_subdir = src/backend/distributed/cdc
citus_top_builddir = ../../../..
citus_decoders_dir = $(DESTDIR)$(pkglibdir)/citus_decoders

OBJS += cdc_decoder.o cdc_decoder_utils.o

include $(citus_top_builddir)/Makefile.global

override CFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include
override CPPFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include

install: install-cdc

clean: clean-cdc

install-cdc:
	mkdir -p '$(citus_decoders_dir)'
	$(INSTALL_SHLIB) citus_$(DECODER).so '$(citus_decoders_dir)/$(DECODER).so'

clean-cdc:
	rm -f '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'
