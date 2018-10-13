/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for ODBC
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * Copyright (c) 2016, 2017, 2018, CARTO
 *
 * This software is released under the PostgreSQL Licence
 *
 * Original author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *                odbc_fdw/odbc_fdw--0.2.0.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION odbc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION odbc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER odbc_fdw
  HANDLER odbc_fdw_handler
  VALIDATOR odbc_fdw_validator;

CREATE TYPE __tabledata AS (schema text, name text);

CREATE FUNCTION ODBCTablesList(text, integer DEFAULT 0) RETURNS SETOF __tabledata
AS 'MODULE_PATHNAME', 'odbc_tables_list'
LANGUAGE C STRICT;

CREATE FUNCTION ODBCTableSize(text, text) RETURNS INTEGER
AS 'MODULE_PATHNAME', 'odbc_table_size'
LANGUAGE C STRICT;

CREATE FUNCTION ODBCQuerySize(text, text) RETURNS INTEGER
AS 'MODULE_PATHNAME', 'odbc_query_size'
LANGUAGE C STRICT;
