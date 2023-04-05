citus_top_builddir = ../../../..
include $(citus_top_builddir)/Makefile.global

citus_subdir = src/backend/distributed/cdc
SRC_DIR = $(citus_abs_top_srcdir)/$(citus_subdir)

#List of supported based decoders. Add new decoders here.
cdc_base_decoders :=pgoutput wal2json

all: build-cdc-decoders

copy-decoder-files-to-build-dir:
	$(eval DECODER_BUILD_DIR=build-cdc-$(DECODER))
	mkdir -p $(DECODER_BUILD_DIR)
	@for file in $(SRC_DIR)/*.c $(SRC_DIR)/*.h; do \
		if [ -f $$file ]; then \
			if [ -f $(DECODER_BUILD_DIR)/$$(basename $$file) ]; then \
				if ! diff -q $$file $(DECODER_BUILD_DIR)/$$(basename $$file); then \
					cp $$file $(DECODER_BUILD_DIR)/$$(basename $$file); \
				fi \
			else \
				cp $$file $(DECODER_BUILD_DIR)/$$(basename $$file); \
			fi \
		fi \
	done
	cp $(SRC_DIR)/Makefile.decoder $(DECODER_BUILD_DIR)/Makefile

build-cdc-decoders:
	$(foreach base_decoder,$(cdc_base_decoders),$(MAKE) DECODER=$(base_decoder) build-cdc-decoder;)

install-cdc-decoders:
	$(foreach base_decoder,$(cdc_base_decoders),$(MAKE) DECODER=$(base_decoder) -C build-cdc-$(base_decoder) install;)

clean-cdc-decoders:
	$(foreach base_decoder,$(cdc_base_decoders),rm -rf build-cdc-$(base_decoder);)


build-cdc-decoder:
	$(MAKE) DECODER=$(DECODER) copy-decoder-files-to-build-dir
	$(MAKE) DECODER=$(DECODER) -C build-cdc-$(DECODER)

install: install-cdc-decoders

clean: clean-cdc-decoders

