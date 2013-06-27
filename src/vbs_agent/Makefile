TOP        = ../
SWIG       = swig
SRCS       = vbs_agent.c cJSON.c
TARGET     = vbs_agent
INTERFACE  = vbs_agent.i

all::
	$(MAKE) -f $(TOP)/Makefile SRCS='$(SRCS)' SWIG='$(SWIG)' \
	TARGET='$(TARGET)' INTERFACE='$(INTERFACE)'  LIBS='-L/usr/local/lib -lpthread' python

static::
	$(MAKE) -f $(TOP)/Makefile SRCS='$(SRCS)' SWIG='$(SWIG)' \
	TARGET='mypython' INTERFACE='$(INTERFACE)' python_static

clean::
	$(MAKE) -f $(TOP)/Makefile python_clean
	rm -f $(TARGET).py

check: all
	$(MAKE) -f $(TOP)/Makefile python_run
