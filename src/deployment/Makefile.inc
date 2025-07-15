OBJDIRS += deployment

# Create template specific variables
UPPERC_DIR := DEPLOYMENT
LOWERC_DIR := deployment

DEPLOYMENT_SRCS :=
DEPLOYMENT_PROG := deployment/cluster deployment/db

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS :=

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
# Need to specify test cases explicitly because they have variables in recipe
test-deployment: $(DEPLOYMENT_TESTS)
	@for a in $(DEPLOYMENT_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
	
# These are left over from the deployment Makefile.  I'm sure there's a far
#  less redundant way to incorporate these, but for now they're fine here
$(BINDIR)/deployment/cluster: $(OBJDIR)/deployment/cluster.o \
                              $(OBJDIR)/common/configuration.o
	@echo + ld $@
	@mkdir -p $(@D)
	$(V)$(CXX) -o $@ $^ $(LDFLAGS) -lrt

$(BINDIR)/deployment/db: $(OBJDIR)/deployment/main.o \
                         $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) \
                         $(APPLICATIONS_OBJS) $(SCHEDULER_OBJS) \
                         $(SEQUENCER_OBJS) $(PAXOS_OBJS)
	@echo + ld $@
	@mkdir -p $(@D)
	$(V)$(CXX) -o $@ $^ $(LDFLAGS) -lrt $(ZMQLDFLAGS)
