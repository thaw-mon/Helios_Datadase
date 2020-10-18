AUTOMAKE_OPTIONS=foreign

PWD = $(shell /bin/pwd)/..

noinst_LIBRARIES=libloader.a
libloader_a_SOURCES= DataLoader.cpp
LIBS= -lmpi -lconfig++ -lhiredis -lpthread

AM_CPPFLAGS=-I$(PWD)/utils -I$(PWD)/database  -I$(PWD)/repartition -I$(MPI_CXXFLAGS)  
