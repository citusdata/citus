ifndef DECODER
	DECODER = pgoutput
endif

MODULE_big = citus_$(DECODER)

citus_decoders_dir = $(DESTDIR)$(pkglibdir)/citus_decoders

# Set the default value to citus_top_builddir as relative path to the
# citus top directory from current directory.
ifeq ($(strip $(citus_top_builddir)),)
citus_top_builddir = ../../../..
citus_subdir = $(shell pwd)
endif

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
