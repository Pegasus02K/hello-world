include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dsload_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dsload_main_new.c \
	dsload_version_new.c

include $(SOURCE_BASE)/base/make/rules

