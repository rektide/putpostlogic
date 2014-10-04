short_ver = 0.1.0
long_ver = $(short_ver)-$(shell git describe --long 2>/dev/null || echo unknown)

MODULE_big = putpostlogic
OBJS = decode.o putpostlogic.o

EXTENSION = putpostlogic
PG_CPPFLAGS = $(pkg-config --cflags libnanomsg) $(pkg-config --cflags json-c) -Irt
SHLIB_LIBS = $(pkg-config --libs libnanomsg) $(pkg-config --libs json-c) -lz -llibrdkafka -lrt

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

putpostlogic.control: ext/putpostlogic.control
	sed -e 's,__short_ver__,$(short_ver),g' < $^ > $@

dist:
	git archive --output=putpostlogic_$(long_ver).tar.gz --prefix=putpostlogic HEAD .
