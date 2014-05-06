OBJDIRS += proto

# Create template specific variables
UPPERC_DIR := PROTO
LOWERC_DIR := proto

PROTO_SRCS := proto/*.proto

SRC_LINKED_OBJECTS := $(OBJDIR)/proto/%.cc
TEST_LINKED_OBJECTS :=
	
# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Wierd protobuf stuff for compilation
$(OBJDIR)/proto/%.o: $(OBJDIR)/proto/%.pb.h
$(OBJDIR)/proto/%.pb.cc $(OBJDIR)/proto/%.pb.h: proto/%.proto
	@echo + protoc $<
	@mkdir -p $(@D)
	$(V)$(LDLIBRARYPATH) $(PROTOCEXE) --proto_path=proto --cpp_out=$(OBJDIR)/proto $<
