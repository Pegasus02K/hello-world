include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dstouch_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dstouch_main_new.c \
	dstouch_version_new.c

include $(SOURCE_BASE)/base/make/rules

