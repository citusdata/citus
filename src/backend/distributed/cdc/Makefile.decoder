MODULE_big = citus_$(DECODER)

citus_decoders_dir = $(DESTDIR)$(pkglibdir)/citus_decoders

citus_top_builddir = ../../../../..
citus_subdir = src/backend/distributed/cdc/cdc_$(DECODER)

OBJS += cdc_decoder.o cdc_decoder_utils.o

include $(citus_top_builddir)/Makefile.global

override CFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include
override CPPFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include

install: install-cdc

install-cdc:
	mkdir -p '$(citus_decoders_dir)'
	$(INSTALL_SHLIB) citus_$(DECODER)$(DLSUFFIX) '$(citus_decoders_dir)/$(DECODER)$(DLSUFFIX)'
