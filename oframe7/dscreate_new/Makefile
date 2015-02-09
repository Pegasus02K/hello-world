include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dscreate_new

LIBS = -lofcom -lvolm -ldscom -lams -ldsio -ldsalc -llockm $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dscreate_main_new.c \
	dscreate_version_new.c

include $(SOURCE_BASE)/base/make/rules

