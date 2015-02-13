include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = gdgcreate_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)


SOURCES = \
	gdgcreate_main_new.c \
	gdgcreate_version_new.c

include $(SOURCE_BASE)/base/make/rules

