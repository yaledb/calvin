OBJDIRS += paxos

# Create template specific variables
UPPERC_DIR := PAXOS
LOWERC_DIR := paxos

PAXOS_PROG :=
PAXOS_SRCS := paxos/paxos.cc

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
# Need to specify test cases explicitly because they have variables in recipe
test-paxos: $(PAXOS_TESTS)
	@for a in $(PAXOS_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
