MODULE_big = citus_pgoutput
EXTENSION = citus_pgoutput


OBJS = cdc_decoder.o cdc_decoder_utils.o


SHLIB_LINK = $(libpq)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
INCDIR := $(shell $(PG_CONFIG) --includedir)
include $(PGXS)

override CFLAGS += -std=gnu99 -Wall -Wextra -Wno-unused-parameter -Wno-sign-compare -Wno-missing-field-initializers -Wno-clobbered -Wno-declaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wmissing-declarations -Wmissing-prototypes -Wshadow -Werror=vla -Werror=implicit-int -Werror=implicit-function-declaration -Werror=return-type -fstack-clash-protection