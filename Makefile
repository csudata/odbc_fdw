##########################################################################
#
#                foreign-data wrapper for ODBC
#
# Copyright (c) 2011, PostgreSQL Global Development Group
# Copyright (c) 2016, CARTO
#
# This software is released under the PostgreSQL Licence
#
# Author: Zheng Yang <zhengyang4k@gmail.com>
#
# IDENTIFICATION
#                 odbc_fdw/Makefile
#
##########################################################################

MODULE_big = odbc_fdw
OBJS = odbc_fdw.o odbc_deparse.o odbc_shippable.o

EXTENSION = odbc_fdw
DATA = odbc_fdw--0.3.0.sql \
  odbc_fdw--0.2.0--0.3.0.sql \
  odbc_fdw--0.3.0--0.2.0.sql

SHLIB_LINK = -lodbc

ifdef DEBUG
override CFLAGS += -DDEBUG 
endif
override CFLAGS += -DDIRECT_INSERT  

REGRESS = odbc_fdw

REGRESS_OPTS = --port=65432 --host=127.0.0.1
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CPPFLAGS += -I$(CURDIR)/src/include -I$(shell $(PG_CONFIG) --includedir)
include $(PGXS)
else
override PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include -I$(libpq_srcdir)
subdir = contrib/odbc_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


