OBJDIRS += applications

# Create template specific variables
UPPERC_DIR := APPLICATIONS
LOWERC_DIR := applications

APPLICATIONS_SRCS := applications/tpcc.cc applications/microbenchmark.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS)
TEST_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-applications: $(APPLICATIONS_TESTS)
	@for a in $(APPLICATIONS_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
