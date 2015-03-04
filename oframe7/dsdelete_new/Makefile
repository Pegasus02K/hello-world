include $(SOURCE_BASE)/base/make/cflags

TYPE = tool
MODULE = dsdelete_new

LIBS = -lofcom -ldscom -llogin $(LIBTMAXCLI)
CFLAGS_ADD = -I$(SOURCE_BASE)/base/ds/include -I$(TMAXDIR)

SOURCES = \
	dsdelete_main_new.c \
	dsdelete_version_new.c

include $(SOURCE_BASE)/base/make/rules
include $(SOURCE_BASE)/batch/make/rules
