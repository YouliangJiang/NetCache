ifeq ($(RTE_SDK),)
$(error "Please define RTE_SDK environment variable")
endif

# Default target, can be overriden by command line or environment
RTE_TARGET ?= x86_64-native-linuxapp-gcc

include $(RTE_SDK)/mk/rte.vars.mk

# binary name
APP = backend

# all source are stored in SRCS-y
SRCS-y := backend.c ../../lib/tommyhashdyn.c

CFLAGS += -O3 -I../../../util -I../../../lib # -g #for gdb debugging
#CFLAGS += $(WERROR_FLAGS)

include $(RTE_SDK)/mk/rte.extapp.mk
