OBJDIRS += sequencer

# Create template specific variables
UPPERC_DIR := SEQUENCER
LOWERC_DIR := sequencer

SEQUENCER_PROG :=
SEQUENCER_SRCS := sequencer/sequencer.cc

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-sequencer: $(SEQUENCER_TESTS)
	@for a in $(SEQUENCER_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
