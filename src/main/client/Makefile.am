AUTOMAKE_OPTIONS=foreign

PWD = $(shell /bin/pwd)/../..

bin_PROGRAMS=helios_client
helios_client_SOURCES=client_main.cpp  
helios_client_LDADD=  $(PWD)/loader/libloader.a  $(PWD)/repartition/librepartition.a $(PWD)/utils/libutils.a $(PWD)/database/libdatabase.a $(PWD)/utils/libutils.a
AM_CPPFLAGS=-I$(PWD)/utils -I$(PWD)/database  -I$(PWD)/loader -I/$(PWD)/repartition -I$(MPI_CXXFLAGS)  
helios_client_LDFLAGS=$(MPI_CXXLDFLAGS) -lmpi -lconfig++ -lhiredis -lpthread -ltbb -lboost_serialization -lboost_thread -lboost_date_time -lrt -lzmq 
