include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = gdgdelete_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	gdgdelete_main_new.c \
	gdgdelete_version_new.c

include $(SOURCE_BASE)/base/make/rules

