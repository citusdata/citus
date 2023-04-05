ifndef DECODER
	DECODER = pgoutput
endif

MODULE_big = citus_$(DECODER)

citus_decoders_dir = $(DESTDIR)$(pkglibdir)/citus_decoders

# Set the default value to citus_top_builddir as relative path to the
# citus top directory from current directory.
ifeq ($(strip $(citus_top_builddir)),)
citus_top_builddir = ../../../..
citus_subdir = src/backend/distributed/cdc
endif

OBJS += cdc_decoder.o cdc_decoder_utils.o

include $(citus_top_builddir)/Makefile.global

override CFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include
override CPPFLAGS += -DDECODER=\"$(DECODER)\" -I$(citus_abs_top_srcdir)/include

all: cdc_pgouput cdc_wal2json

copy_decoder_files_to_build_dir:
	mkdir -p $(DECODER_BUILD_DIR)
	@for file in *.c *.h; do \
		if [ -f $$file ]; then \
			if ! diff -q $$file $(DECODER_BUILD_DIR)/$$(basename $$file); then \
				cp $$file $(DECODER_BUILD_DIR)/$$(basename $$file); \
			fi \
		fi \
	done
	cp Makefile.decoder $(DECODER_BUILD_DIR)/Makefile

cdc_pgouput:
	$(MAKE) DECODER_BUILD_DIR=build_pgoutput copy_decoder_files_to_build_dir
	$(MAKE) DECODER=pgoutput -C build_pgoutput

cdc_wal2json:
	$(MAKE) DECODER_BUILD_DIR=build_wal2json copy_decoder_files_to_build_dir
	$(MAKE) DECODER=wal2json -C build_wal2json

install: install-cdc

clean: clean-cdc

install-cdc:
	$(MAKE) DECODER=pgoutput -C build_pgoutput install
	$(MAKE) DECODER=wal2json -C build_wal2json install

clean-cdc:
	rm -rf build_pgoutput build_wal2json
	rm -f '$(DESTDIR)$(datadir)/$(datamoduledir)/citus_decoders/$(DECODER).so'
