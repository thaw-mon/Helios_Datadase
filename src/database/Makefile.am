AUTOMAKE_OPTIONS=foreign

PWD=$(shell /bin/pwd)/..
noinst_LIBRARIES=libdatabase.a
libdatabase_a_SOURCES=QueryEdge.cpp triple_t.cpp  Database.cpp Memory.cpp AsyncRedisStore.cpp AsyncCallBack.cpp  
#libdatabase_LDADD=  $(PWD)/utils/libutils.a
LIBS = -lhiredis -lboost_serialization
AM_CPPFLAGS = -I$(PWD)/utils -I$(PWD)/repartition  
