# Changelog

## 0.3.0
Released 2018-10-20
Bug fixes:
- connection leak
- Avoid send "select count(*) from ft" to get table statistic, which will hang remote database if table is huge

Announcements:
- support push down projection, where condition, order by to remote table
- support push down group by to remote table for PG 10
- reuse some code of postgres_fdw

Released 2018-02-20

Bug fixes:
- Fixed issues with travis builds
- elog_debug: Avoid warnings when disabled 0a8b95a
- Avoid unsigned/signed comparison warnings 763d70e

Announcements:
- Added support for PostgreSQL versions 9.6 and 10, and future v11.
- Changed to apache hive 2.2.1 in travis builds
- Updated README.md with supported driver versions 4ede641
- Added CONTRIBUTING.md document
- Added an `.editorconfig` file to help enforce formatting of c/h/sql/yml files 222b39a
- Applied bulk formatting pass to get everything lined up d53480e
- Added this NEWS.md file
- Added a release procedure in HOWTO_RELEASE.md file


## 0.2.0
Released 2016-09-30

Bug fixes:
- Fixed missing schema option `OPTION` a3b43b0

Announcements:
- Added test capabilities for all connectors
- Added support for schema-less ODBC data sources (e.g. Hive) 109557a
- Updated `freetds` package version to `1.00.14cdb7`
- Added ODBCTablesList function to query for the list of tables the user has access to in the server
- Added ODBCTableSize function to get the size, in rows, of the foreign table
- Added ODBCQuerySize function to get the size, in rows, of the provided query


## 0.1.0-rc1
Released 2016-08-03

Bug fixes:
- Quote connection attributes #15
- Handle single quotes when quoting options #19
- Prevent memory leak and race conditions d52fd60
- Handle partial SQLGetData results 3db51c0
- Use adequate minimum buffer size for numeric data to avoid precission loss df59364
- Fix various binary column problems 4caff4f

Announcements:
- Allows definition of arbitrary ODBC attributes with `odbc_` options 778ae02
- Limits size of varying columns and buffers 8149e32


## 0.0.1
Released 2016-07-15

First version based off https://github.com/bluthg/odbc_fdw at 0d44e9d. Additionally, it provides the following:

Bug fixes:
- Fixed compilation issues and API mismatches
- Fixed bug causing segfaults with query columns not present in foreign table
- Many other fixes for typos, NULL values, pointers, lenght of params, etc.

Announcements:
- Minimum PostgreSQL supported version updated to 9.5 and removed support for older versions
- Updated build instructions
- Added license file
- Updated README file
- Added driver, host and port parameters
- Added tests for the build `PGUSER=postgres make installcheck`
- Added support for `IMPORT FOREIGN SCHEMA`
- Added support for Add for no `sql_query` and no `sql_count` in options cases
- Added `encoding` option
- Allows username and password in server definition
- Added support for `GUID` columns
