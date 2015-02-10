include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dsmove_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dsmove_main_new.c \
	dsmove_version_new.c

include $(SOURCE_BASE)/base/make/rules

