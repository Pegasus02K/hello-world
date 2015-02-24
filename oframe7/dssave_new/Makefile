include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dssave_new

LIBS = -lofcom -ldscom $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dssave_main_new.c \
	dssave_version_new.c

include $(SOURCE_BASE)/base/make/rules

