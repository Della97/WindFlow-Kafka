# Author: Gabriele Mencagli
# Date: 11/01/2020

FF_ROOT			= $(HOME)/git/fastflow
WF_INCLUDES		= $(HOME)/git/WindFlow-Kafka/wf
INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread -lrdkafka++

all: fd generator

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

generator.o: generator.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

generator: generator.o
	$(CXX) generator.o -o ../bin/generator $(LDFLAGS)

fd.o: fd.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

fd: metric.o metric_group.o sampler.o fd.o
	$(CXX) metric.o metric_group.o sampler.o fd.o -o ../bin/fd $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/fd
	rm -f $(OUT_DIR)/generator
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
