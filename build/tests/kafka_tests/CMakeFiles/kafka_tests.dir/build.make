# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.20

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/della/git/WindFlow-Kafka

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/della/git/WindFlow-Kafka/build

# Utility rule file for kafka_tests.

# Include any custom commands dependencies for this target.
include tests/kafka_tests/CMakeFiles/kafka_tests.dir/compiler_depend.make

# Include the progress variables for this target.
include tests/kafka_tests/CMakeFiles/kafka_tests.dir/progress.make

kafka_tests: tests/kafka_tests/CMakeFiles/kafka_tests.dir/build.make
.PHONY : kafka_tests

# Rule to build all files generated by this target.
tests/kafka_tests/CMakeFiles/kafka_tests.dir/build: kafka_tests
.PHONY : tests/kafka_tests/CMakeFiles/kafka_tests.dir/build

tests/kafka_tests/CMakeFiles/kafka_tests.dir/clean:
	cd /home/della/git/WindFlow-Kafka/build/tests/kafka_tests && $(CMAKE_COMMAND) -P CMakeFiles/kafka_tests.dir/cmake_clean.cmake
.PHONY : tests/kafka_tests/CMakeFiles/kafka_tests.dir/clean

tests/kafka_tests/CMakeFiles/kafka_tests.dir/depend:
	cd /home/della/git/WindFlow-Kafka/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/della/git/WindFlow-Kafka /home/della/git/WindFlow-Kafka/tests/kafka_tests /home/della/git/WindFlow-Kafka/build /home/della/git/WindFlow-Kafka/build/tests/kafka_tests /home/della/git/WindFlow-Kafka/build/tests/kafka_tests/CMakeFiles/kafka_tests.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : tests/kafka_tests/CMakeFiles/kafka_tests.dir/depend

