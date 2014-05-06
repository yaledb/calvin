#
# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf
#

# We create a listing of the root directories for access into
OBJDIR := ../obj
BINDIR := ../bin
EXTDIR := ../ext
SRCDIR := ../src
DBDIR  := ../db
CHKPNT := ../db/checkpoints
STORE  := ../db/storage

# Next, we define the external libraries we use and their directories
ZEROMQ := $(EXTDIR)/zeromq
PROTOB := $(EXTDIR)/protobuf
GTEST  := $(EXTDIR)/googletest
ZOOKPR := $(EXTDIR)/zookeeper

# Executable for protocol buffers
PROTOCEXE := $(PROTOB)/src/.libs/protoc

# '$(V)' controls whether the lab makefiles print verbose commands (the
# actual shell commands run by Make), as well as the "overview" commands
# (such as '+ cc lib/readline.c').
#
# For overview commands only, the line should read 'V = @'.
# For overview and verbose commands, the line should read 'V ='.
V = @

# Set the compiler and compile-time loaded libraries
CXX           := g++
LDLIBRARYPATH := LD_LIBRARY_PATH=$(ZEROMQ)/src/.libs:$(PROTOB)/src/.libs:$(GTEST)/lib/.libs:$(ZOOKPR)/.libs

# For profiling (with gprof), this line should read 'PG = -pg'
# For fast execution, this line should read 'PG ='.
PG = 

# Set the flags for C++ to compile with (namely where to look for external
# libraries) and the linker libraries (again to look in the ext/ library)
CXXFLAGS := -g -MD $(PG) -I$(SRCDIR) -I$(OBJDIR) \
            -I$(ZEROMQ)/include \
            -I$(PROTOB)/src \
            -I$(GTEST)/include \
            -I$(ZOOKPR)/include -I$(ZOOKPR)/generated
CXXFLAGS += -Wall -Werror

LDFLAGS := -lrt -lpthread $(PG) \
           -lprotobuf -L$(PROTOB)/src/.libs \
           -lgtest -L$(GTEST)/lib/.libs \
           -lzookeeper_mt -L$(ZOOKPR)/.libs
ZMQLDFLAGS := -lzmq -L$(ZEROMQ)/src/.libs

# Lists that the */Makefile.inc makefile fragments will add to
OBJDIRS :=
TESTS :=

# Make sure that 'all' is the first target
all:
	@echo + Ensuring Database Storage Layer Exists...
	@mkdir -p $(STORE)
	@echo + Ensuring Checkpoint Storage Layer Exists...
	@mkdir -p $(CHKPNT)
	
# Makefile template so that the makefrags are far less redundant
MAKEFILE_TEMPLATE := Makefile.template

# Makefile fragments for library code
include proto/Makefile.inc
include common/Makefile.inc
include backend/Makefile.inc
include paxos/Makefile.inc

# Makefile fragments for components
include applications/Makefile.inc
include scheduler/Makefile.inc
include sequencer/Makefile.inc

# Makefile fragments for executable and deployment scripts
include deployment/Makefile.inc

test: $(TESTS)

clean:
	rm -rf $(OBJDIR) $(BINDIR) $(DBDIR) $(STORE) $(CHKPNT)

# This magic automatically generates makefile dependencies
# for header files included from C source files we compile,
# and keeps those dependencies up-to-date every time we recompile.
# See 'mergedep.pl' for more information.
$(OBJDIR)/.deps: $(foreach dir, $(OBJDIRS), $(wildcard $(OBJDIR)/$(dir)/*.d))
	@mkdir -p $(@D)
	@cat $^ /dev/null > $@
#	@$(PERL) mergedep.pl $@ $^
-include $(OBJDIR)/.deps

always:

# Eliminate default suffix rules
.SUFFIXES:

# make it so that no intermediate .o files are ever deleted
.PRECIOUS: $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.o) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.cc) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.h)

.PHONY: all always clean test
