AUTOMAKE_OPTIONS = foreign
#ACLOCAL_AMFLAGS = -I m4

PWD = $(shell /bin/pwd)/..
noinst_LIBRARIES = libutils.a
libutils_a_SOURCES = type.c HashFunction.c 

AM_CPPFLAGS= -I$(PWD)/database -I$(PWD)/helios_db -I$(PWD)/libredis
