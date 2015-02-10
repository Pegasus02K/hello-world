include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dscopy_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dscopy_main_new.c \
	dscopy_version_new.c

include $(SOURCE_BASE)/base/make/rules

