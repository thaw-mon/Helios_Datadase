#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.65)
AC_INIT(helios, 1.0, liuchen@cmail.carleton.ca)
AM_INIT_AUTOMAKE(helios, 1.0)
#AC_CONFIG_MACRO_DIR(m4)
#AC_CONFIG_SRCDIR([utils/HashFunction.cpp])
#AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

#AC_PROG_LIBTOOL
AC_PROG_RANLIB

# Checks for libraries.
#LT_INIT

# Checks for header files.
#AC_CHECK_HEADERS([stdint.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
#AC_HEADER_STDBOOL

# Checks for library functions.

CFLAGS="-g -fPIC -gdwarf-2 -std=gnu99 -pg"
CXXFLAGS="-g -fopenmp -gdwarf-2 -std=c++11 -pg"
#AC_CONFIG_FILES([utils/Makefile])
AC_OUTPUT(Makefile
		src/database/Makefile
                 src/loader/Makefile
                 src/repartition/Makefile
                 src/utils/Makefile
		 src/main/server/Makefile
		 src/main/client/Makefile)
