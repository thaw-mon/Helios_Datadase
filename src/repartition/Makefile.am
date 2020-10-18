AUTOMAKE_OPTIONS=foreign

PWD = $(shell /bin/pwd)/..

noinst_LIBRARIES=librepartition.a
librepartition_a_SOURCES=Repartitioner.cpp 

LIBS = -lhiredis
AM_CPPFLAGS = -I$(PWD)/utils -I$(PWD)/database 
