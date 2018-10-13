/*----------------------------------------------------------
 *
 *        foreign-data wrapper for ODBC
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 * Updated to 9.2+ by Gunnar "Nick" Bluth <nick@pro-open.de>
 *   based on tds_fdw code from Geoff Montee
 * Updated to 9.6 10  www.cstech.ltd 
 * IDENTIFICATION
 *      odbc_fdw/odbc_fdw.c
 *
 *----------------------------------------------------------
 */
#include "postgres.h"
#include "odbc_fdw.h"

#include <string.h>
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/relcache.h"
#include "utils/numeric.h"
#include "storage/lock.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"

#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "executor/spi.h"

#include <stdio.h>
#include <sql.h>
#include <sqlext.h>


PG_MODULE_MAGIC;

/* Macro to make conditional DEBUG more terse */
#ifdef DEBUG
#define elog_debug(...) elog(DEBUG1, __VA_ARGS__)
#else
#define elog_debug(...) ((void) 0)
#endif

#define PROCID_TEXTEQ 67
#define PROCID_TEXTCONST 25

/* Provisional limit to name lengths in characters */
#define MAXIMUM_CATALOG_NAME_LEN 255
#define MAXIMUM_SCHEMA_NAME_LEN 255
#define MAXIMUM_TABLE_NAME_LEN 255
#define MAXIMUM_COLUMN_NAME_LEN 255

/* Maximum GetData buffer size */
#define MAXIMUM_BUFFER_SIZE 8192
/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/* Default remote table size */
#define DEFAULT_TABLE_SIZE 1000000

/*
 * Numbers of the columns returned by SQLTables:
 * 1: TABLE_CAT (ODBC 3.0) TABLE_QUALIFIER (ODBC 2.0) -- database name
 * 2: TABLE_SCHEM (ODBC 3.0) TABLE_OWNER (ODBC 2.0)   -- schema name
 * 3: TABLE_NAME
 * 4: TABLE_TYPE
 * 5: REMARKS
 */
#define SQLTABLES_SCHEMA_COLUMN 2
#define SQLTABLES_NAME_COLUMN 3

#define ODBC_SQLSTATE_FRACTIONAL_TRUNCATION "01S07"
typedef struct odbcFdwOptions
{
	char  *schema;     /* Foreign schema name */
	char  *table;      /* Foreign table */
	char  *prefix;     /* Prefix for imported foreign table names */
	char  *sql_query;  /* SQL query (overrides table) */
	char  *sql_count;  /* SQL query for counting results */
	char  *encoding;   /* Character encoding name */
	bool  updatable;   /* table can be update */
	List *connection_list; /* ODBC connection attributes */

	List  *mapping_list; /* Column name mapping */
} odbcFdwOptions;

/**
 * ODBC Execution state of a foreign scan 
 */
typedef struct odbcFdwExecutionState
{
	Relation        rel;
	TupleDesc       tupdesc;
	AttInMetadata   *attinmeta;
	List			*retrieved_attrs;    /* attr numbers retrieved by RETURNING */

	odbcFdwOptions  options;
	SQLHSTMT        stmt;
	SQLHDBC			conn;
	int             num_of_result_cols;
	bool            first_iteration;
	List            *col_position_mask;
	List            *col_size_array;
	List            *col_conversion_array;
	char            *sql_count;
	int             encoding;
} odbcFdwExecutionState;

/*
 * ODBC Execution state of a foreign insert/update/delete operation.
 */
typedef struct odbcFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* for remote query execution */
	odbcFdwOptions  options;
	SQLHSTMT        stmt;
	SQLHDBC			conn;

	bool			prepared; 
	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */
	
	FmgrInfo   *p_inputflinfo;		/* output conversion functions for odbc */
	Oid			*p_input_typIOParam;
	int32 		*p_input_typmod;
	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} odbcFdwModifyState;

/*
 * Execution state of a foreign scan that modifies a foreign table directly.
 */
typedef struct odbcFdwDirectModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of UPDATE/DELETE command */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */
	bool		set_processed;	/* do we set the command es_processed? */
	SQLHDBC		conn;

	/* for remote query execution */
	SQLHSTMT    stmt;			/* connection for the update */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */

	/* for storing result tuples */
	int			num_tuples;		/* # of result tuples */
	int			next_tuple;		/* index of next one to return */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} odbcFdwDirectModifyState;


struct odbcFdwOption
{
	const char   *optname;
	Oid     optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Array of valid options
 * In addition to this, any option with a name prefixed
 * by odbc_ is accepted as an ODBC connection attribute
 * and can be defined in foreign servier, user mapping or
 * table statements.
 * Note that dsn and driver can be defined by
 * prefixed or non-prefixed options.
 */
static struct odbcFdwOption valid_options[] =
{
	/* Foreign server options */
	{ "dsn",        ForeignServerRelationId },
	{ "driver",     ForeignServerRelationId },
	{ "encoding",   ForeignServerRelationId },
	{ "updatable", 	ForeignServerRelationId },

	/* Foreign table options */
	{ "schema",     ForeignTableRelationId },
	{ "table",      ForeignTableRelationId },
	{ "prefix",     ForeignTableRelationId },
	{ "sql_query",  ForeignTableRelationId },
	{ "sql_count",  ForeignTableRelationId },
	{ "updatable", 	ForeignTableRelationId },

	/* Sentinel */
	{ NULL,       InvalidOid}
};
static SQLHENV odbc_env = NULL;

enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

enum FdwDirectModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwDirectModifyPrivateUpdateSql,
	/* has-returning flag (as an integer Value node) */
	FdwDirectModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwDirectModifyPrivateRetrievedAttrs,
	/* set-processed flag (as an integer Value node) */
	FdwDirectModifyPrivateSetProcessed
};

typedef enum 
{ 
	TEXT_CONVERSION, 
	HEX_CONVERSION,
	BIN_CONVERSION, 
	BOOL_CONVERSION 
} ColumnConversion;

/*
 * SQL functions
 */
extern Datum odbc_fdw_handler(PG_FUNCTION_ARGS);
extern Datum odbc_fdw_validator(PG_FUNCTION_ARGS);
extern Datum odbc_tables_list(PG_FUNCTION_ARGS);
extern Datum odbc_table_size(PG_FUNCTION_ARGS);
extern Datum odbc_query_size(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(odbc_fdw_handler);
PG_FUNCTION_INFO_V1(odbc_fdw_validator);
PG_FUNCTION_INFO_V1(odbc_tables_list);
PG_FUNCTION_INFO_V1(odbc_table_size);
PG_FUNCTION_INFO_V1(odbc_query_size);

/*
 * FDW callback routines
 */
static void odbcExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void odbcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *odbcIterateForeignScan(ForeignScanState *node);
static void odbcReScanForeignScan(ForeignScanState *node);
static void odbcEndForeignScan(ForeignScanState *node);
static void odbcGetForeignRelSize(PlannerInfo *root,
								  RelOptInfo *baserel, 
								  Oid foreigntableid);

static void odbcGetForeignPaths(PlannerInfo *root,
								RelOptInfo *baserel,
								Oid foreigntableid);

static ForeignScan* odbcGetForeignPlan(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid, 
										ForeignPath *best_path, 
										List *tlist, 
										List *scan_clauses, 
										Plan *outer_plan);

List* odbcImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);

static List *odbcPlanForeignModify(PlannerInfo *root,
							   	   ModifyTable *plan,
							       Index resultRelation,
							       int subplan_index);

static void odbcBeginForeignModify(ModifyTableState *mtstate,
								   ResultRelInfo *resultRelInfo,
						   		   List *fdw_private,
						   		   int subplan_index,
						   		   int eflags);

static TupleTableSlot *odbcExecForeignInsert(EState *estate,
											 ResultRelInfo *resultRelInfo,
						  					 TupleTableSlot *slot,
						  					 TupleTableSlot *planSlot);

static TupleTableSlot *odbcExecForeignUpdate(EState *estate,
						  					 ResultRelInfo *resultRelInfo,
						  					 TupleTableSlot *slot,
						  					 TupleTableSlot *planSlot);

static TupleTableSlot *odbcExecForeignDelete(EState *estate,
						  					 ResultRelInfo *resultRelInfo,
						  					 TupleTableSlot *slot,
											 TupleTableSlot *planSlot);

static void odbcEndForeignModify(EState *estate,
						 		 ResultRelInfo *resultRelInfo);

static int	odbcIsForeignRelUpdatable(Relation rel);

static bool odbcPlanDirectModify(PlannerInfo *root,
						 		 ModifyTable *plan,
						 		 Index resultRelation,
						 		 int subplan_index);

static void odbcBeginDirectModify(ForeignScanState *node, int eflags);
static TupleTableSlot *odbcIterateDirectModify(ForeignScanState *node);
static void odbcEndDirectModify(ForeignScanState *node);
static void odbcGetForeignUpperPaths(PlannerInfo *root,
				     UpperRelationKind stage,
				     RelOptInfo *input_rel,
				     RelOptInfo *output_rel);


/*
 * helper functions
 */
static bool odbcIsValidOption(const char *option, Oid context);
static void check_return(SQLRETURN ret, 
						 char *msg,
						 SQLHANDLE handle,
						 SQLSMALLINT type);
static const char* empty_string_if_null(char *string);
static void extract_odbcFdwOptions(List *options_list, 
								   odbcFdwOptions *extracted_options);
static void init_odbcFdwOptions(odbcFdwOptions* options);
static void copy_odbcFdwOptions(odbcFdwOptions* to, odbcFdwOptions* from);
static void odbc_connection(odbcFdwOptions* options, SQLHDBC *dbc);
static void sql_data_type(SQLSMALLINT odbc_data_type,
						  SQLULEN column_size, 
						  SQLSMALLINT decimal_digits,
						  SQLSMALLINT nullable,
						  StringInfo sql_type);

static void odbcGetOptions(Oid server_oid, List *add_options,
						   odbcFdwOptions *extracted_options);

static void odbcGetTableOptions(Oid foreigntableid, odbcFdwOptions *extracted_options);
static void odbcGetTableSize(odbcFdwOptions* options, unsigned int *size);
static void check_return(SQLRETURN ret, char *msg, SQLHANDLE handle, SQLSMALLINT type);
static void odbcConnStr(StringInfoData *conn_str, odbcFdwOptions* options);
static char* get_schema_name(odbcFdwOptions *options);
static inline bool is_blank_string(const char *s);
static Oid oid_from_server_name(char *serverName);
static void odbc_prepare_foreign_modify(odbcFdwModifyState *fmstate);
static void odbc_Bind_Prepared(odbcFdwModifyState *fmstate, char** p_value);

static const char** odbc_convert_prep_stmt_params(odbcFdwModifyState *fmstate,
						 						  ItemPointer tupleid,
						 						  TupleTableSlot *slot);

static bool ec_member_matches_foreign(PlannerInfo *root,
									  RelOptInfo *rel,
									  EquivalenceClass *ec,
									  EquivalenceMember *em,
									  void *arg);

static void add_paths_with_pathkeys_for_rel(PlannerInfo *root,
                                            RelOptInfo *rel,
                                            Path *epq_path);

static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
											  RelOptInfo *rel);

static List * get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);

static void estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *foreignrel,
									List *param_join_conds,
									List *pathkeys,
									double *p_rows, int *p_width,
									Cost *p_startup_cost, Cost *p_total_cost);
#if PG_VERSION_NUM >= 100000
static void add_foreign_grouping_paths(PlannerInfo *root,
					RelOptInfo *input_rel,
					RelOptInfo *grouped_rel);

static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
#endif

static void merge_fdw_options(PgFdwRelationInfo *fpinfo,
				  const PgFdwRelationInfo *fpinfo_o,
				  const PgFdwRelationInfo *fpinfo_i);

#ifdef DIRECT_INSERT
char *build_insert_sql(odbcFdwModifyState *fmstate, char** p_value);
#endif

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * Check if string pointer is NULL or points to empty string
 */
static inline bool is_blank_string(const char *s)
{
	return s == NULL || s[0] == '\0';
}

Datum
odbc_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	/* FIXME */
	fdwroutine->GetForeignRelSize = odbcGetForeignRelSize;
	fdwroutine->GetForeignPaths = odbcGetForeignPaths;
	fdwroutine->AnalyzeForeignTable = NULL;
	fdwroutine->GetForeignPlan = odbcGetForeignPlan;
	fdwroutine->ExplainForeignScan = odbcExplainForeignScan;
	fdwroutine->BeginForeignScan = odbcBeginForeignScan;
	fdwroutine->IterateForeignScan = odbcIterateForeignScan;
	fdwroutine->ReScanForeignScan = odbcReScanForeignScan;
	fdwroutine->EndForeignScan = odbcEndForeignScan;

	fdwroutine->PlanDirectModify = odbcPlanDirectModify;
	fdwroutine->BeginDirectModify = odbcBeginDirectModify;
	fdwroutine->IterateDirectModify = odbcIterateDirectModify;
	fdwroutine->EndDirectModify = odbcEndDirectModify;

	/* Functions for updating foreign tables */

	fdwroutine->PlanForeignModify = odbcPlanForeignModify;
	fdwroutine->BeginForeignModify = odbcBeginForeignModify;
	fdwroutine->ExecForeignInsert = odbcExecForeignInsert;
	fdwroutine->ExecForeignUpdate = odbcExecForeignUpdate;
	fdwroutine->ExecForeignDelete = odbcExecForeignDelete;

	fdwroutine->EndForeignModify = odbcEndForeignModify;

	fdwroutine->IsForeignRelUpdatable = odbcIsForeignRelUpdatable;

	fdwroutine->ImportForeignSchema = odbcImportForeignSchema;
#if PG_VERSION_NUM >= 100000
	fdwroutine->GetForeignUpperPaths = odbcGetForeignUpperPaths;
#endif
	PG_RETURN_POINTER(fdwroutine);
}

static void
init_odbcFdwOptions(odbcFdwOptions* options)
{
	memset(options, 0, sizeof(odbcFdwOptions));
}

static void
copy_odbcFdwOptions(odbcFdwOptions* to, odbcFdwOptions* from)
{
	if (to && from)
	{
		*to = *from;
	}
}

/*
 * Avoid NULL string: return original string, or empty string if NULL
 */
static const char*
empty_string_if_null(char *string)
{
	static const char* empty_string = "";
	return string == NULL ? empty_string : string;
}

static const char   odbc_attribute_prefix[] = "odbc_";

/*  strlen(odbc_attribute_prefix); */
static const size_t odbc_attribute_prefix_len = sizeof(odbc_attribute_prefix) - 1; 

static bool
is_odbc_attribute(const char* defname)
{
	return (strlen(defname) > odbc_attribute_prefix_len &&
			strncmp(defname, odbc_attribute_prefix, odbc_attribute_prefix_len) == 0);
}

/* These ODBC attributes names are always uppercase */
static const char *normalized_attributes[] = { "DRIVER", "DSN", "UID", "PWD" };
static const char *normalized_attribute(const char* attribute_name)
{
	size_t i;
	for (i=0; i < sizeof(normalized_attributes)/sizeof(normalized_attributes[0]); i++)
	{
		if (strcasecmp(attribute_name, normalized_attributes[i])==0)
		{
			attribute_name = normalized_attributes[i];
			break;
		}
	}
	return 	attribute_name;
}

static const char*
get_odbc_attribute_name(const char* defname)
{
	int offset = is_odbc_attribute(defname) ? odbc_attribute_prefix_len : 0;
	return normalized_attribute(defname + offset);
}

static void
extract_odbcFdwOptions(List *options_list, odbcFdwOptions *extracted_options)
{
	ListCell        *lc;

	elog_debug("%s", __func__);

	init_odbcFdwOptions(extracted_options);

	/* Loop through the options, and get the foreign table options */
	foreach(lc, options_list)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "dsn") == 0)
		{
			extracted_options->connection_list =
									lappend(extracted_options->connection_list, def);
			continue;
		}

		if (strcmp(def->defname, "driver") == 0)
		{
			extracted_options->connection_list =
									lappend(extracted_options->connection_list, def);
			continue;
		}

		if (strcmp(def->defname, "schema") == 0)
		{
			extracted_options->schema = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "table") == 0)
		{
			extracted_options->table = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "prefix") == 0)
		{
			extracted_options->prefix = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "sql_query") == 0)
		{
			extracted_options->sql_query = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "sql_count") == 0)
		{
			extracted_options->sql_count = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "encoding") == 0)
		{
			extracted_options->encoding = defGetString(def);
			continue;
		}

		if (strcmp(def->defname, "updatable") == 0)
		{
			extracted_options->updatable = defGetBoolean(def);
			continue;
		}

		/* Column mapping goes here */
		/* TODO: is this useful? if so, how can columns names coincident
		   with option names be escaped? */
		extracted_options->mapping_list = 
								lappend(extracted_options->mapping_list, def);
	}
}

/*
 * Get the schema name from the options
 */
static char* get_schema_name(odbcFdwOptions *options)
{
	return options->schema;
}

/*
 * Establish ODBC connection
 */
static void
odbc_connection(odbcFdwOptions* options, SQLHDBC *dbc)
{
	StringInfoData  conn_str;
	SQLCHAR OutConnStr[1024];
	SQLSMALLINT OutConnStrLen;
	SQLRETURN ret;
	
	odbcConnStr(&conn_str, options);
	if (odbc_env == NULL) {
		/* Allocate an environment handle */
		SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &odbc_env);
		/* We want ODBC 3 support */
		SQLSetEnvAttr(odbc_env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
	}

	/* Allocate a connection handle */
	SQLAllocHandle(SQL_HANDLE_DBC, odbc_env, dbc);
	/* Connect to the DSN */
	ret = SQLDriverConnect(*dbc, NULL, (SQLCHAR *) conn_str.data, SQL_NTS,
	                       OutConnStr, 1024, &OutConnStrLen, SQL_DRIVER_COMPLETE);
	check_return(ret, "Connecting to driver", dbc, SQL_HANDLE_DBC);
}

/*
 * Validate function
 */
Datum
odbc_fdw_validator(PG_FUNCTION_ARGS)
{
	List  *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid   catalog = PG_GETARG_OID(1);
	char  *svr_schema   = NULL;
	char  *svr_table    = NULL;
	char  *svr_prefix   = NULL;
	char  *sql_query    = NULL;
	char  *sql_count    = NULL;
	ListCell *cell;

	elog_debug("%s", __func__);

	/*
	 * Check that the necessary options: address, port, database
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/* Complain invalid options */
		if (!odbcIsValidOption(def->defname, catalog))
		{
			struct odbcFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
					                 opt->optname);
			}

			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			         errmsg("invalid option \"%s\"", def->defname),
			         errhint("Valid options in this context are: %s",
								buf.len ? buf.data : "<none>")
			        ));
		}

		/* TODO: detect redundant connection attributes and missing required 
 		 * attributs (dsn or driver)
		 * Complain about redundent options
		 */
		if (strcmp(def->defname, "schema") == 0)
		{
			if (!is_blank_string(svr_schema))
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: schema (%s)", 
								defGetString(def))
				        ));

			svr_schema = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (!is_blank_string(svr_table))
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: table (%s)",
								 defGetString(def))
				        ));

			svr_table = defGetString(def);
		}
		else if (strcmp(def->defname, "prefix") == 0)
		{
			if (!is_blank_string(svr_prefix))
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: prefix (%s)",
								 defGetString(def))
				        ));

			svr_prefix = defGetString(def);
		}
		else if (strcmp(def->defname, "sql_query") == 0)
		{
			if (sql_query)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: sql_query (%s)",
								 defGetString(def))
				        ));

			sql_query = defGetString(def);
		}
		else if (strcmp(def->defname, "sql_count") == 0)
		{
			if (!is_blank_string(sql_count))
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: sql_count (%s)",
								 defGetString(def))
				        ));

			sql_count = defGetString(def);
		}
		else if (strcmp(def->defname, "updatable") == 0)
		{
			 (void)defGetBoolean(def);
		}
	}

	PG_RETURN_VOID();
}

/*
 * Map ODBC data types to PostgreSQL
 */
static void
sql_data_type(SQLSMALLINT odbc_data_type,
			  SQLULEN     column_size,
			  SQLSMALLINT decimal_digits,
			  SQLSMALLINT nullable,
			  StringInfo  sql_type)
{
	initStringInfo(sql_type);
	switch(odbc_data_type)
	{
	case SQL_CHAR:
	case SQL_WCHAR :
		appendStringInfo(sql_type, "char(%u)", (unsigned)column_size);
		break;
	case SQL_VARCHAR :
	case SQL_WVARCHAR :
		if (column_size <= 255)
		{
			appendStringInfo(sql_type, "varchar(%u)", (unsigned)column_size);
		}
		else
		{
			appendStringInfo(sql_type, "text");
		}
		break;
	case SQL_LONGVARCHAR :
	case SQL_WLONGVARCHAR :
		appendStringInfo(sql_type, "text");
		break;
	case SQL_DECIMAL :
		appendStringInfo(sql_type, "decimal(%u,%d)", (unsigned)column_size,
						 decimal_digits);
		break;
	case SQL_NUMERIC :
		appendStringInfo(sql_type, "numeric(%u,%d)", (unsigned)column_size,
						 decimal_digits);
		break;
	case SQL_INTEGER :
		appendStringInfo(sql_type, "integer");
		break;
	case SQL_REAL :
		appendStringInfo(sql_type, "real");
		break;
	case SQL_FLOAT :
		appendStringInfo(sql_type, "real");
		break;
	case SQL_DOUBLE :
		appendStringInfo(sql_type, "float8");
		break;
	case SQL_BIT :
		/* Use boolean instead of bit(1) because:
		 * * binary types are not yet fully supported
		 * * boolean is more commonly used in PG
		 * * With options BoolsAsChar=0 this allows
		 *   preserving boolean columns from pSQL ODBC.
		 */
		appendStringInfo(sql_type, "boolean");
		break;
	case SQL_SMALLINT :
	case SQL_TINYINT :
		appendStringInfo(sql_type, "smallint");
		break;
	case SQL_BIGINT :
		appendStringInfo(sql_type, "bigint");
		break;
	/*
	 * TODO: Implement these cases properly. See #23
	 *
	case SQL_BINARY :
		appendStringInfo(sql_type, "bit(%u)", (unsigned)column_size);
		break;
	case SQL_VARBINARY :
		appendStringInfo(sql_type, "varbit(%u)", (unsigned)column_size);
		break;
	*/
	case SQL_LONGVARBINARY :
		appendStringInfo(sql_type, "bytea");
		break;
	case SQL_TYPE_DATE :
	case SQL_DATE :
		appendStringInfo(sql_type, "date");
		break;
	case SQL_TYPE_TIME :
	case SQL_TIME :
		appendStringInfo(sql_type, "time");
		break;
	case SQL_TYPE_TIMESTAMP :
	case SQL_TIMESTAMP :
		appendStringInfo(sql_type, "timestamp");
		break;
	case SQL_GUID :
		appendStringInfo(sql_type, "uuid");
		break;
	};
}

static SQLULEN
minimum_buffer_size(SQLSMALLINT odbc_data_type)
{
	switch(odbc_data_type)
	{
	case SQL_DECIMAL :
	case SQL_NUMERIC :
		return 32;
	case SQL_INTEGER :
		return 12;
	case SQL_REAL :
	case SQL_FLOAT :
		return 18;
	case SQL_DOUBLE :
		return 26;
	case SQL_SMALLINT :
	case SQL_TINYINT :
		return 6;
	case SQL_BIGINT :
		return 21;
	case SQL_TYPE_DATE :
	case SQL_DATE :
		return 10;
	case SQL_TYPE_TIME :
	case SQL_TIME :
		return 8;
	case SQL_TYPE_TIMESTAMP :
	case SQL_TIMESTAMP :
		return 20;
	default :
		return 0;
	};
}

/*
 * Fetch the options for a server and options list
 */
static void
odbcGetOptions(Oid server_oid,
			   List *add_options,
			   odbcFdwOptions *extracted_options)
{
	ForeignServer   *server;
	UserMapping     *mapping;
	List            *options;

	elog_debug("%s", __func__);

	server  = GetForeignServer(server_oid);
	mapping = GetUserMapping(GetUserId(), server_oid);

	options = NIL;
	options = list_concat(options, add_options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	extract_odbcFdwOptions(options, extracted_options);
}

/*
 * Fetch the options for a odbc_fdw foreign table.
 */
static void
odbcGetTableOptions(Oid foreigntableid, odbcFdwOptions *extracted_options)
{
	ForeignTable    *table;

	elog_debug("%s", __func__);

	table = GetForeignTable(foreigntableid);
	odbcGetOptions(table->serverid, table->options, extracted_options);
}

static void
check_return(SQLRETURN ret, char *msg, SQLHANDLE handle, SQLSMALLINT type)
{
	if (SQL_SUCCEEDED(ret))
		elog(DEBUG1, "Successful result: %s", msg);
	else
	{
		int err_code = ERRCODE_SYSTEM_ERROR;
		SQLINTEGER   i = 0;
		SQLINTEGER   native;
		SQLCHAR  state[ 7 ];
		SQLCHAR  text[256];
		SQLSMALLINT  len;
		SQLRETURN    diag_ret;
		StringInfoData  err_str;

		initStringInfo(&err_str);
		elog(DEBUG1, "Error result (%d): %s", ret, msg);
		if (handle)
		{
			do
			{
				diag_ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
				                         sizeof(text), &len);
				if (SQL_SUCCEEDED(diag_ret)) 
					appendBinaryStringInfo(&err_str, text, len);
			}
			while( diag_ret == SQL_SUCCESS );
			ereport(ERROR, (errcode(err_code), errmsg("%s", err_str.data)));
		} else {
			ereport(ERROR, (errcode(err_code), errmsg("%s", msg)));
		} 
	}
}

/*
 * Get name qualifier char
 */
static void
getNameQualifierChar(SQLHDBC dbc, StringInfoData *nq_char)
{
	SQLCHAR name_qualifier_char[2];

	elog_debug("%s", __func__);

	SQLGetInfo(dbc,
	           SQL_CATALOG_NAME_SEPARATOR,
	           (SQLPOINTER)&name_qualifier_char,
	           2,
	           NULL);
	name_qualifier_char[1] = 0; // some drivers fail to copy the trailing zero

	initStringInfo(nq_char);
	appendStringInfo(nq_char, "%s", (char *) name_qualifier_char);
}

/*
 * Get quote cahr
 */
static void
getQuoteChar(SQLHDBC dbc, StringInfoData *q_char)
{
	SQLCHAR quote_char[2];

	elog_debug("%s", __func__);

	SQLGetInfo(dbc,
	           SQL_IDENTIFIER_QUOTE_CHAR,
	           (SQLPOINTER)&quote_char,
	           2,
	           NULL);
	quote_char[1] = 0; // some drivers fail to copy the trailing zero

	initStringInfo(q_char);
	appendStringInfo(q_char, "%s", (char *) quote_char);
}

static bool
appendConnAttribute(bool sep, StringInfoData *conn_str,
					const char* name, const char* value)
{
	static const char *sep_str = ";";
	if (!is_blank_string(value))
	{
		if (sep)
			appendStringInfoString(conn_str, sep_str);
		appendStringInfo(conn_str, "%s=%s", name, value);
		sep = true;
	}
	return sep;
}

static void
odbcConnStr(StringInfoData *conn_str, odbcFdwOptions* options)
{
	bool sep = false;
	ListCell *lc;

	initStringInfo(conn_str);

	foreach(lc, options->connection_list)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		sep = appendConnAttribute(sep, conn_str, 
									get_odbc_attribute_name(def->defname),
									defGetString(def));
	}
	elog_debug("CONN STR: %s", conn_str->data);
}

/*
 * get table size of a table
 */
static void
odbcGetTableSize(odbcFdwOptions* options, unsigned int *size)
{
	SQLHDBC dbc;
	SQLHSTMT stmt;
	SQLRETURN ret;

	StringInfoData  sql_str;

	SQLUBIGINT table_size;
	SQLLEN indicator;

	StringInfoData name_qualifier_char;
	StringInfoData quote_char;

	const char* schema_name;

	schema_name = get_schema_name(options);

	odbc_connection(options, &dbc);

	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

	if (is_blank_string(options->sql_count))
	{
		/* Get quote char */
		getQuoteChar(dbc, &quote_char);

		/* Get name qualifier char */
		getNameQualifierChar(dbc, &name_qualifier_char);

		initStringInfo(&sql_str);
		if (is_blank_string(options->sql_query))
		{
			if (is_blank_string(schema_name))
			{
				appendStringInfo(&sql_str, "SELECT COUNT(*) FROM %s%s%s",
				                 quote_char.data, options->table, quote_char.data);
			}
			else
			{
				appendStringInfo(&sql_str, "SELECT COUNT(*) FROM %s%s%s%s%s%s%s",
				                 quote_char.data, schema_name, quote_char.data,
				                 name_qualifier_char.data,
				                 quote_char.data, options->table, quote_char.data);
			}
		}
		else
		{
			if (options->sql_query[strlen(options->sql_query)-1] == ';')
			{
				/* Remove trailing semicolon if present */
				options->sql_query[strlen(options->sql_query)-1] = 0;
			}
			appendStringInfo(&sql_str, 
							 "SELECT COUNT(*) FROM (%s) AS _odbc_fwd_count_wrapped", 
							 options->sql_query);
		}
	}
	else
	{
		initStringInfo(&sql_str);
		appendStringInfo(&sql_str, "%s", options->sql_count);
	}

	elog_debug("Count query: %s", sql_str.data);

	ret = SQLExecDirect(stmt, (SQLCHAR *) sql_str.data, SQL_NTS);
	check_return(ret, "Executing ODBC query", stmt, SQL_HANDLE_STMT);
	if (SQL_SUCCEEDED(ret))
	{
		SQLFetch(stmt);
		/* retrieve column data as a big int */
		ret = SQLGetData(stmt, 1, SQL_C_UBIGINT, &table_size, 0, &indicator);
		if (SQL_SUCCEEDED(ret))
		{
			*size = (unsigned int) table_size;
			elog_debug("Count query result: %lu", table_size);
		}
	}
	else
	{
		elog(WARNING, "Error getting the table %s size", options->table);
	}

	/* Free handles, and disconnect */
	if (stmt)
	{
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		stmt = NULL;
	}

	if (dbc)
	{
		SQLDisconnect(dbc);
		SQLFreeHandle(SQL_HANDLE_DBC, dbc);
		dbc = NULL;
	}

	if (odbc_env)
	{
		SQLFreeHandle(SQL_HANDLE_ENV, odbc_env);
		odbc_env = NULL;
	}
}

static int
strtoint(const char *nptr, char **endptr, int base)
{
	long val = strtol(nptr, endptr, base);
	return (int) val;
}

static Oid 
oid_from_server_name(char *serverName)
{
	char *serverOidString;
	char sql[1024];
	int serverOid;
	HeapTuple tuple;
	TupleDesc tupdesc;
	int ret;

	if ((ret = SPI_connect()) < 0) {
		elog(ERROR, "oid_from_server_name: SPI_connect returned %d", ret);
	}

	sprintf(sql, "SELECT oid FROM pg_foreign_server where srvname = '%s'", serverName);
	if ((ret = SPI_execute(sql, true, 1)) != SPI_OK_SELECT) {
		elog(ERROR, 
			 "oid_from_server_name: Get server name from Oid query Failed, SP_exec returned %d.",
			 ret);
	}

	if (SPI_tuptable->vals[0] != NULL)
	{
		tupdesc  = SPI_tuptable->tupdesc;
		tuple    = SPI_tuptable->vals[0];

		serverOidString = SPI_getvalue(tuple, tupdesc, 1);
		serverOid = strtoint(serverOidString, NULL, 10);
	} else {
		elog(ERROR, "Foreign server %s doesn't exist", serverName);
	}

	SPI_finish();
	return serverOid;
}

Datum
odbc_table_size(PG_FUNCTION_ARGS)
{
	Oid serverOid;
	unsigned int tableSize;
	odbcFdwOptions options;

	char *serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *tableName = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *defname = "table";
	List *tableOptions = NIL;
	Node *val = (Node *) makeString(tableName);
#if PG_VERSION_NUM >= 100000
	DefElem *elem = (DefElem *) makeDefElem(defname, val, -1);
#else
	DefElem *elem = (DefElem *) makeDefElem(defname, val);
#endif

	tableOptions = lappend(tableOptions, elem);
	serverOid = oid_from_server_name(serverName);
	odbcGetOptions(serverOid, tableOptions, &options);
	odbcGetTableSize(&options, &tableSize);

	PG_RETURN_INT32(tableSize);
}

Datum
odbc_query_size(PG_FUNCTION_ARGS)
{

	odbcFdwOptions options;
	unsigned int querySize;
	Oid serverOid;
 
	char *serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *sqlQuery = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *defname = "sql_query";
	List *queryOptions = NIL;
	Node *val = (Node *) makeString(sqlQuery);
#if PG_VERSION_NUM >= 100000
	DefElem *elem = (DefElem *) makeDefElem(defname, val, -1);
#else
	DefElem *elem = (DefElem *) makeDefElem(defname, val);
#endif
	queryOptions = lappend(queryOptions, elem);
	serverOid = oid_from_server_name(serverName);
	odbcGetOptions(serverOid, queryOptions, &options);
	odbcGetTableSize(&options, &querySize);

	PG_RETURN_INT32(querySize);
}

/*
 * Get the list of tables for the current datasource
 */
typedef struct {
	SQLSMALLINT TargetType;
	SQLPOINTER TargetValuePtr;
	SQLINTEGER BufferLength;
	SQLLEN StrLen_or_Ind;
} DataBinding;

typedef struct {
	Oid serverOid;
	DataBinding* tableResult;
    SQLHDBC dbc;
	SQLHSTMT stmt;
	SQLCHAR schema;
	SQLCHAR name;
	SQLUINTEGER rowLimit;
	SQLUINTEGER currentRow;
} TableDataCtx;

Datum 
odbc_tables_list(PG_FUNCTION_ARGS)
{
	SQLHDBC dbc;
	SQLHSTMT stmt;
	SQLUSMALLINT i;
	SQLUSMALLINT numColumns = 5;
	SQLUSMALLINT bufferSize = 1024;
	SQLUINTEGER rowLimit;
	SQLUINTEGER currentRow;
	SQLRETURN retCode;

	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	TableDataCtx *datafctx;
	DataBinding* tableResult;
	AttInMetadata *attinmeta;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		char *serverName;
		int serverOid;
		odbcFdwOptions options;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		datafctx = (TableDataCtx *) palloc(sizeof(TableDataCtx));
		tableResult = (DataBinding*) palloc(numColumns * sizeof(DataBinding));

		serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
		serverOid = oid_from_server_name(serverName);

		rowLimit = PG_GETARG_INT32(1);
		currentRow = 0;

		odbcGetOptions(serverOid, NULL, &options);
		odbc_connection(&options, &dbc);
		SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

		for ( i = 0 ; i < numColumns ; i++ ) {
			tableResult[i].TargetType = SQL_C_CHAR;
			tableResult[i].BufferLength = (bufferSize + 1);
			tableResult[i].TargetValuePtr = 
									palloc(sizeof(char)*tableResult[i].BufferLength);
		}

		for ( i = 0 ; i < numColumns ; i++ ) {
			retCode = SQLBindCol(stmt, i + 1, tableResult[i].TargetType, 
								 tableResult[i].TargetValuePtr,
								 tableResult[i].BufferLength, 
								 &(tableResult[i].StrLen_or_Ind));
		}

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			         errmsg("function returning record called in context "
			                "that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);

		datafctx->serverOid = serverOid;
		datafctx->tableResult = tableResult;
		datafctx->stmt = stmt;
		datafctx->dbc = dbc;
		datafctx->rowLimit = rowLimit;
		datafctx->currentRow = currentRow;
		funcctx->user_fctx = datafctx;
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	datafctx = funcctx->user_fctx;
	stmt = datafctx->stmt;
	tableResult = datafctx->tableResult;
	rowLimit = datafctx->rowLimit;
	currentRow = datafctx->currentRow;
	attinmeta = funcctx->attinmeta;

	retCode = SQLTables(stmt, NULL, SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS,
						(SQLCHAR*)"TABLE", SQL_NTS );
	if (SQL_SUCCEEDED(retCode = SQLFetch(stmt)) &&
		(rowLimit == 0 || currentRow < rowLimit)) {
		char       **values;
		HeapTuple    tuple;
		Datum        result;

		values = (char **) palloc(2 * sizeof(char *));
		values[0] = (char *) palloc(256 * sizeof(char));
		values[1] = (char *) palloc(256 * sizeof(char));
		snprintf(values[0], 256, "%s",
				 (char *)tableResult[SQLTABLES_SCHEMA_COLUMN-1].TargetValuePtr);
		snprintf(values[1], 256, "%s",
				 (char *)tableResult[SQLTABLES_NAME_COLUMN-1].TargetValuePtr);
		tuple = BuildTupleFromCStrings(attinmeta, values);
		result = HeapTupleGetDatum(tuple);
		currentRow++;
		datafctx->currentRow = currentRow;
		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		SQLDisconnect(datafctx->dbc);
		SQLFreeHandle(SQL_HANDLE_DBC, datafctx->dbc);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
odbcIsValidOption(const char *option, Oid context)
{
	struct odbcFdwOption *opt;

	elog_debug("%s", __func__);

	/* Check if the options presents in the valid option list */
	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}

	/* ODBC attributes are valid in any context */
	if (is_odbc_attribute(option))
	{
		return true;
	}

	/* Foreign table may have anything as a mapping option */
	if (context == ForeignTableRelationId)
		return true;
	else
		return false;
}

static void
odbcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo;
	ListCell   *lc;
	odbcFdwOptions options;
	const char *namespace;
	const char *relname;
	const char *refname;

	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	elog_debug("%s", __func__);

	/* Fetch the foreign table options */
	odbcGetTableOptions(foreigntableid, &options);

	baserel->rows = DEFAULT_TABLE_SIZE;
	baserel->tuples = baserel->rows;
	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;

	fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	odbc_classifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */

	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost);
	}

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(foreigntableid));
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;

}


static void odbcGetForeignPaths(PlannerInfo *root,
								RelOptInfo *baserel,
								Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;
	List	   *ppi_list;
	ListCell   *lc;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * It's convenient to manage this by representing each candidate outer
	 * relation by the ParamPathInfo node for it.  We can then use the
	 * ppi_clauses list in the ParamPathInfo node directly as a list of the
	 * interesting join clauses for that rel.  This takes care of the
	 * possibility that there are multiple safe join clauses for such a rel,
	 * and also ensures that we account for unsafe join clauses that we'll
	 * still have to enforce locally (since the parameterized-path machinery
	 * insists that we handle all movable clauses).
	 */
	ppi_list = NIL;
	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Relids		required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, baserel))
			continue;

		/* See if it is safe to send to remote */
		if (!odbc_is_foreign_expr(root, baserel, rinfo->clause))
			continue;

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids,
								   baserel->lateral_relids);
		/* We do not want the foreign rel itself listed in required_outer */
		required_outer = bms_del_member(required_outer, baserel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
			continue;

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, baserel,
											   required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (baserel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the foreign rel.  Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List	   *clauses;

			/* Make clauses, skipping any that join to lateral_referencers */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 baserel,
															 ec_member_matches_foreign,
															 (void *) &arg,
															 baserel->lateral_referencers);

			/* Done if there are no more expressions in the foreign rel */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach(lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
				Relids		required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, baserel))
					continue;

				/* See if it is safe to send to remote */
				if (!odbc_is_foreign_expr(root, baserel, rinfo->clause))
					continue;

				/* Calculate required outer rels for the resulting path */
				required_outer = bms_union(rinfo->clause_relids,
										   baserel->lateral_relids);
				required_outer = bms_del_member(required_outer, baserel->relid);
				if (bms_is_empty(required_outer))
					continue;

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, baserel,
													   required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/* Try again, now ignoring the expression we found this time */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	/*
	 * Now build a path for each useful outer relation.
	 */
	foreach(lc, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *) lfirst(lc);
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;

		/* Get a cost estimate from the remote */
		estimate_path_cost_size(root, baserel,
								param_info->ppi_clauses, NIL,
								&rows, &width,
								&startup_cost, &total_cost);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = create_foreignscan_path(root, baserel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   param_info->ppi_req_outer,
									   NULL,
									   NIL);	/* no fdw_private list */
		add_path(baserel, (Path *) path);
	}
}


static ForeignScan* odbcGetForeignPlan(PlannerInfo *root,
									   RelOptInfo *foreignrel,
									   Oid foreigntableid,
									   ForeignPath *best_path,
									   List *tlist,
									   List *scan_clauses,
									   Plan *outer_plan)
{	
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by postgresGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (odbc_is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = odbc_build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot. This is safe because all scans and
		 * joins support projection, so we never need to insert a Result node.
		 * Also, remove the local conditions from outer plan's quals, lest
		 * they will be evaluated twice, once by the local plan and once by
		 * the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			outer_plan->targetlist = fdw_scan_tlist;

			foreach(lc, local_exprs)
			{
				Join	   *join_plan = (Join *) outer_plan;
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.
				 */
				if (join_plan->jointype == JOIN_INNER)
					join_plan->joinqual = list_delete(join_plan->joinqual,
													  qual);
			}
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	odbc_deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
							remote_exprs, best_path->path.pathkeys,
							false, &retrieved_attrs, &params_list);

	/* Remember remote_exprs for possible use by postgresPlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make3(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(fpinfo->fetch_size));
	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);

}

/*
 * odbcBeginForeignScan
 *
 */
static void
odbcBeginForeignScan(ForeignScanState *node, int eflags)
{
	SQLHDBC dbc;
	odbcFdwExecutionState   *festate;
	SQLSMALLINT result_columns;
	SQLHSTMT stmt;
	SQLRETURN ret;

#ifdef DEBUG
	char dsn[256];
	char desc[256];
	SQLSMALLINT dsn_ret;
	SQLSMALLINT desc_ret;
	SQLUSMALLINT direction;
#endif
	
	int				rtindex;
	odbcFdwOptions 	options;
	char	   		*query;
	Relation 		rel;
	RangeTblEntry 	*rte;
	int 			num_of_columns;
	StringInfoData 	*columns;
	int 			i;
	ListCell 		*col_mapping;
	StringInfoData 	col_str;
	StringInfoData 	name_qualifier_char;
	StringInfoData 	quote_char;
	ForeignScan 	*fsplan = (ForeignScan *) node->ss.ps.plan;
	EState			*estate = node->ss.ps.state;
	int 			encoding = -1;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	elog_debug("%s", __func__);
	query = strVal(list_nth(fsplan->fdw_private,
									 FdwScanPrivateSelectSql));
	elog_debug("%s", query);

	/* Fetch the foreign table options */
	if (fsplan->scan.scanrelid > 0)
 		rtindex = fsplan->scan.scanrelid; 
	else 
		rtindex = bms_next_member(fsplan->fs_relids, -1);

	rte = rt_fetch(rtindex, estate->es_range_table);

	odbcGetTableOptions(rte->relid, &options);

	odbc_connection(&options,  &dbc);

	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

	/* Retrieve a list of rows */
	ret = SQLExecDirect(stmt, (SQLCHAR *) query, SQL_NTS);
	check_return(ret, "Executing ODBC query", stmt, SQL_HANDLE_STMT);
	SQLNumResultCols(stmt, &result_columns);

	festate = (odbcFdwExecutionState *) palloc(sizeof(odbcFdwExecutionState));
	if (fsplan->scan.scanrelid > 0)
	{
		festate->rel = node->ss.ss_currentRelation;
		festate->tupdesc = RelationGetDescr(festate->rel);                                          
	}
	else
    	{
		festate->rel = NULL;           
		festate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}
	festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);
	copy_odbcFdwOptions(&(festate->options), &options);
	festate->stmt = stmt;
	festate->conn = dbc;
	festate->num_of_result_cols = result_columns;
	/* prepare for the first iteration, there will be some precalculation needed in the first iteration*/
	festate->first_iteration = true;
	festate->encoding = encoding;
	node->fdw_state = (void *) festate;
}

/*
 * odbcIterateForeignScan
 *
 */
static TupleTableSlot *
odbcIterateForeignScan(ForeignScanState *node)
{
	TupleDesc       tupdesc;
	EState *executor_state = node->ss.ps.state;
	MemoryContext prev_context;
	/* ODBC API return status */
	SQLRETURN ret;
	odbcFdwExecutionState *festate = (odbcFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	//SQLSMALLINT columns;
	Datum   	*values;
	bool		*nulls;
	HeapTuple   tuple;
	StringInfoData  col_data;
	SQLHSTMT stmt = festate->stmt;
	bool first_iteration = festate->first_iteration;
	int num_of_result_columns = festate->num_of_result_cols;
	List *col_position_mask = NIL;
	List *col_size_array = NIL;
	List *col_conversion_array = NIL;

	elog_debug("%s", __func__);

	if (festate->rel)
		tupdesc = RelationGetDescr(festate->rel);
	else
	{
		tupdesc = festate->tupdesc; 
	}

	ret = SQLFetch(stmt);

	/*
	 * If this is the first iteration,
	 * we need to calculate the mask for column mapping as well as the column size
	 */
	if (first_iteration == true)
	{
		SQLCHAR *ColumnName;
		SQLSMALLINT NameLengthPtr;
		SQLSMALLINT DataTypePtr;
		SQLULEN     ColumnSizePtr;
		SQLSMALLINT DecimalDigitsPtr;
		SQLSMALLINT NullablePtr;
		int i;
		int k;
		bool found;

		StringInfoData sql_type;
		SQLULEN min_size;
		SQLULEN max_size = MAXIMUM_BUFFER_SIZE;
		/* Allocate memory for the masks in a memory context that
		   persists between IterateForeignScan calls */
		prev_context = MemoryContextSwitchTo(executor_state->es_query_cxt);
		col_position_mask = NIL;
		col_size_array = NIL;
		col_conversion_array = NIL;
		/* Obtain the column information of the first row. */
		for (i = 1; i <= num_of_result_columns; i++)
		{
			ColumnConversion conversion = TEXT_CONVERSION;
			found = false;
			ColumnName = (SQLCHAR *) palloc(sizeof(SQLCHAR) * MAXIMUM_COLUMN_NAME_LEN);
			SQLDescribeCol(stmt,
			               i,                       /* ColumnName */
			               ColumnName,
			               sizeof(SQLCHAR) * MAXIMUM_COLUMN_NAME_LEN, /* BufferLength */
			               &NameLengthPtr,
			               &DataTypePtr,
			               &ColumnSizePtr,
			               &DecimalDigitsPtr,
			               &NullablePtr);

			sql_data_type(DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr,
						  &sql_type);
			if (strcmp("bytea", (char*)sql_type.data) == 0)
			{
				conversion = HEX_CONVERSION;
			}
			if (strcmp("boolean", (char*)sql_type.data) == 0)
			{
				conversion = BOOL_CONVERSION;
			}
			else if (strncmp("bit(",(char*)sql_type.data,4)==0 ||
					 strncmp("varbit(",(char*)sql_type.data,7)==0)
			{
				conversion = BIN_CONVERSION;
			}

			min_size = minimum_buffer_size(DataTypePtr);
			
			col_position_mask = lappend_int(col_position_mask, i-1);
			if (ColumnSizePtr < min_size)
				ColumnSizePtr = min_size;
			if (ColumnSizePtr > max_size)
				ColumnSizePtr = max_size;

			col_size_array = lappend_int(col_size_array, (int) ColumnSizePtr);
			col_conversion_array = lappend_int(col_conversion_array, (int) conversion);

			pfree(ColumnName);
		}

		festate->col_position_mask = col_position_mask;
		festate->col_size_array = col_size_array;
		festate->col_conversion_array = col_conversion_array;
		festate->first_iteration = false;

		MemoryContextSwitchTo(prev_context);
	}
	else
	{
		col_position_mask = festate->col_position_mask;
		col_size_array = festate->col_size_array;
		col_conversion_array = festate->col_conversion_array;
	}

	ExecClearTuple(slot);
	if (SQL_SUCCEEDED(ret))
	{
		SQLSMALLINT i;
		ListCell	*lc;
		int			j;

		values = (Datum *)palloc(sizeof(Datum) * tupdesc->natts );
		nulls = (bool *) palloc(sizeof(bool) * tupdesc->natts );
		memset(nulls, true, tupdesc->natts * sizeof(bool));
		/* Loop through the num_of_result_columns */
		i = 0;
		foreach(lc,festate->retrieved_attrs)
		{
			SQLLEN indicator;
			char * buf;

			j = lfirst_int(lc);
			int col_size = list_nth_int(col_size_array, i);
			int mapped_pos = list_nth_int(col_position_mask, i);
			ColumnConversion conversion = list_nth_int(col_conversion_array, i);

			/* Ignore this column if position is marked as invalid */
			if (mapped_pos == -1)
				continue;

			buf = (char *) palloc(sizeof(char) * (col_size+1));

			/* retrieve column data as a zero-terminated string */
			/* TODO:
			   binary fields (SQL_C_BIT, SQL_C_BINARY) do not have
			   a trailing zero; they should be copied as now but without
			   adding 1 to col_size, or using SQL_C_BIT or SQL_C_BINARY
			   and then encoded into a binary PG literal (e.g. X'...'
			   or B'...')
			   For floating point types we should use SQL_C_FLOAT/SQL_C_DOUBLE
			   to avoid precision loss.
			   For date/time/timestamp these structures can be used:
			   SQL_C_TYPE_DATE/SQL_C_TYPE_TIME/SQL_C_TYPE_TIMESTAMP.
			   And finally, SQL_C_NUMERIC and SQL_C_GUID could also be used.
			*/
			buf[0] = 0;
			ret = SQLGetData(stmt, i+1, SQL_C_CHAR,
			                 buf, sizeof(char) * (col_size+1), &indicator);

			if (ret == SQL_SUCCESS_WITH_INFO)
			{
				SQLCHAR sqlstate[5];
				SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlstate, NULL, NULL, 0, NULL);
				if (strcmp((char*)sqlstate, ODBC_SQLSTATE_FRACTIONAL_TRUNCATION) == 0)
				{
					/* Fractional truncation has occured;
					 * at this point we cannot obtain the lost digits
					 */
					if (buf[col_size])
					{
						/* The driver has omitted the trailing */
						char *buf2 = (char *) palloc(sizeof(char) * (col_size+2));
						strncpy(buf2, buf, col_size+1);
						buf2[col_size+1] = 0;
						pfree(buf);
						buf = buf2;
					}
					elog(NOTICE,"Truncating number: %s",buf);
				}
				else
				{
					/* The output is incomplete, we need to obtain the rest of the data */
					char* accum_buffer;
					size_t accum_buffer_size;
					size_t accum_used = 0;
					if (indicator == SQL_NO_TOTAL)
					{
						/* Unknown total size, must copy part by part */
						accum_buffer_size = 0;
						accum_buffer = NULL;
						while (1)
						{
							size_t buf_len = buf[col_size] ? col_size + 1 : col_size;
							// Allocate new accumulation buffer if necessary
							if (accum_used + buf_len > accum_buffer_size)
							{
								char *new_buff;
								accum_buffer_size = accum_buffer_size == 0 ?
													 col_size*2 : accum_buffer_size*2;
								new_buff = (char *) palloc(sizeof(char) * (accum_buffer_size+1));
								if (accum_buffer)
								{
									memmove(new_buff, accum_buffer, accum_used);
									pfree(accum_buffer);
								}
								accum_buffer = new_buff;
								accum_buffer[accum_used] = 0;
							}
							// Copy part to the accumulation buffer
							strncpy(accum_buffer+accum_used, buf, buf_len);
							accum_used += buf_len;
							accum_buffer[accum_used] = 0;
							// Get new part
							if (ret != SQL_SUCCESS_WITH_INFO)
								break;
							ret = SQLGetData(stmt, i+1, SQL_C_CHAR, buf,
											 sizeof(char) * (col_size+1), &indicator);
						};

					}
					else
					{
						/* We need to retrieve indicator more characters */
						size_t buf_len = buf[col_size] ? col_size + 1 : col_size;
						accum_buffer_size = buf_len + indicator;
						accum_buffer = (char *) palloc(sizeof(char)*(accum_buffer_size+1));
						strncpy(accum_buffer, buf, buf_len);
						accum_buffer[buf_len] = 0;
						ret = SQLGetData(stmt, i+1, SQL_C_CHAR, accum_buffer+buf_len, 
										 sizeof(char)*(indicator+1), &indicator);
					}
					pfree(buf);
					buf = accum_buffer;
				}
			}

			if (SQL_SUCCEEDED(ret))
			{
				/* Handle null columns */
				if (indicator == SQL_NULL_DATA)
				{
					// BuildTupleFromCStrings expects NULLs to be NULL pointers
					values[j-1] = (Datum)NULL;
				}
				else
				{
					if (festate->encoding != -1)
					{
						/* Convert character encoding */
						buf = pg_any_to_server(buf, strlen(buf), festate->encoding);
					}

					initStringInfo(&col_data);
					switch (conversion)
					{
						case TEXT_CONVERSION :
						appendStringInfoString (&col_data, buf);
						break;
						case HEX_CONVERSION :
						appendStringInfoString (&col_data, "\\x");
						appendStringInfoString (&col_data, buf);
						break;
						case BOOL_CONVERSION :
						if (buf[0] == 0)
							strcpy(buf, "F");
						else if (buf[0] == 1)
							strcpy(buf, "T");
						appendStringInfoString (&col_data, buf);
						break;
						case BIN_CONVERSION :
						ereport(ERROR,
						        (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						         errmsg("Bit string columns are not supported")
						        ));
						break;
					}
					nulls[j-1] = false;
					values[j-1] =  InputFunctionCall(&(festate->attinmeta->attinfuncs[j - 1]),
															col_data.data,
															festate->attinmeta->attioparams[j - 1],
															festate->attinmeta->atttypmods[j - 1]);
				}
			}
			pfree(buf);
			i++;
		}

		tuple  = heap_form_tuple(tupdesc, values, nulls);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		pfree(values);
	}

	return slot;
}

/*
 * odbcExplainForeignScan
 *
 */
static void
odbcExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	odbcFdwExecutionState *festate;

	elog_debug("%s", __func__);

	festate = (odbcFdwExecutionState *) node->fdw_state;

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		ExplainPropertyLong("Foreign Table Size", DEFAULT_TABLE_SIZE, es);
	}

	if (es->verbose)
	{
		List       *fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private; 
		char	   *sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);                                                                         
        }
}

/*
 * odbcEndForeignScan
 *      Finish scanning foreign table and dispose objects used for this scan
 */
static void
odbcEndForeignScan(ForeignScanState *node)
{
	odbcFdwExecutionState *festate;

	elog_debug("%s", __func__);

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	festate = (odbcFdwExecutionState *) node->fdw_state;
	if (festate)
	{
		if (festate->stmt)
		{
			SQLFreeHandle(SQL_HANDLE_STMT, festate->stmt);
			festate->stmt = NULL;
		}

		if (festate->conn)
		{
			SQLDisconnect(festate->conn);
			SQLFreeHandle(SQL_HANDLE_DBC, festate->conn);
			festate->conn = NULL;
		}
	}
}

/*
 * odbcReScanForeignScan
 *      Rescan table, possibly with new parameters
 */
static void
odbcReScanForeignScan(ForeignScanState *node)
{
	elog(ERROR, "should not be here, TBD");
}

static void
appendQuotedString(StringInfo buffer, const char* text)
{
	static const char SINGLE_QUOTE = '\'';
	const char *p;

	appendStringInfoChar(buffer, SINGLE_QUOTE);

	while (*text)
	{
		p = text;
		while (*p && *p != SINGLE_QUOTE)
		{
			p++;
		}
		appendBinaryStringInfo(buffer, text, p - text);
		if (*p == SINGLE_QUOTE)
		{
			appendStringInfoChar(buffer, SINGLE_QUOTE);
			appendStringInfoChar(buffer, SINGLE_QUOTE);
			p++;
		}
		text = p;
	}

	appendStringInfoChar(buffer, SINGLE_QUOTE);
}

static void
appendOption(StringInfo str, bool first, const char* option_name, 
			 const char* option_value)
{
	if (!first)
	{
		appendStringInfo(str, ",\n");
	}
	appendStringInfo(str, "\"%s\" ", option_name);
	appendQuotedString(str, option_value);
}

/*
 * odbcPlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
static bool
odbcPlanDirectModify(PlannerInfo *root,
					 ModifyTable *plan,
					 Index resultRelation,
					 int subplan_index)
{
	CmdType		operation = plan->operation;
	Plan	   *subplan;
	RelOptInfo *foreignrel;
	RangeTblEntry *rte;
	PgFdwRelationInfo *fpinfo;
	Relation	rel;
	StringInfoData sql;
	ForeignScan *fscan;
	List	   *targetAttrs = NIL;
	List	   *remote_exprs;
	List	   *params_list = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;

	/*
	 * Decide whether it is safe to modify a foreign table directly.
	 */

	/*
	 * The table modification must be an UPDATE or DELETE.
	 */
	if (operation != CMD_UPDATE && operation != CMD_DELETE)
		return false;

	/*
	 * It's unsafe to modify a foreign table directly if there are any local
	 * joins needed.
	 */
	subplan = (Plan *) list_nth(plan->plans, subplan_index);
	if (!IsA(subplan, ForeignScan))
		return false;
	fscan = (ForeignScan *) subplan;

	/*
	 * It's unsafe to modify a foreign table directly if there are any quals
	 * that should be evaluated locally.
	 */
	if (subplan->qual != NIL)
		return false;

	/*
	 * We can't handle an UPDATE or DELETE on a foreign join for now.
	 */
	if (fscan->scan.scanrelid == 0)
		return false;

	/* Safe to fetch data about the target foreign rel */
	foreignrel = root->simple_rel_array[resultRelation];
	rte = root->simple_rte_array[resultRelation];
	fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;

	/*
	 * It's unsafe to update a foreign table directly, if any expressions to
	 * assign to the target columns are unsafe to evaluate remotely.
	 */
	if (operation == CMD_UPDATE)
	{
		int			col;

		/*
		 * We transmit only columns that were explicitly targets of the
		 * UPDATE, so as to avoid unnecessary data transmission.
		 */
		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;
			TargetEntry *tle;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			tle = get_tle_by_resno(subplan->targetlist, attno);

			if (!tle)
				elog(ERROR, "attribute number %d not found in subplan targetlist",
					 attno);

			if (!odbc_is_foreign_expr(root, foreignrel, (Expr *) tle->expr))
				return false;

			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Ok, rewrite subplan so as to modify the foreign table directly.
	 */
	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * Recall the qual clauses that must be evaluated remotely.  (These are
	 * bare clauses not RestrictInfos, but deparse.c's appendConditions()
	 * doesn't care.)
	 */
	remote_exprs = fpinfo->final_remote_exprs;

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_UPDATE:
			odbc_deparseDirectUpdateSql(&sql, root, resultRelation, rel,
								   ((Plan *) fscan)->targetlist,
								   targetAttrs,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		case CMD_DELETE:
			odbc_deparseDirectDeleteSql(&sql, root, resultRelation, rel,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	/*
	 * Update the operation info.
	 */
	fscan->operation = operation;

	/*
	 * Update the fdw_exprs list that will be available to the executor.
	 */
	fscan->fdw_exprs = params_list;

	/*
	 * Update the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwDirectModifyPrivateIndex, above.
	 */
	fscan->fdw_private = list_make4(makeString(sql.data),
									makeInteger((retrieved_attrs != NIL)),
									retrieved_attrs,
									makeInteger(plan->canSetTag));

	heap_close(rel, NoLock);
	return true;
}

/*
 * odbcBeginDirectModify
 *		Prepare a direct foreign table modification
 */
static void
odbcBeginDirectModify(ForeignScanState *node, int eflags)
{
	SQLHDBC dbc;
	SQLHSTMT stmt;
#ifdef DEBUG
	char dsn[256];
	char desc[256];
	SQLSMALLINT dsn_ret;
	SQLSMALLINT desc_ret;
	SQLUSMALLINT direction;
#endif

	odbcFdwOptions options;
	odbcFdwDirectModifyState *dmstate;

	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	dmstate = (odbcFdwDirectModifyState *) palloc0(sizeof(odbcFdwDirectModifyState));
	node->fdw_state = (void *) dmstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */

	/* Get info about foreign table. */
	dmstate->rel = node->ss.ss_currentRelation;
	/* Fetch the foreign table options */
	odbcGetTableOptions(RelationGetRelid(dmstate->rel), &options);
	odbc_connection(&options,  &dbc);
	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

	//-------------------
	dmstate->stmt = stmt;
	dmstate->conn = dbc;

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */

	/* Initialize state variable */
	dmstate->num_tuples = -1;	/* -1 means not set yet */

	/* Get private info created by planner functions. */
	dmstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwDirectModifyPrivateUpdateSql));
	dmstate->has_returning = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateHasReturning));
	dmstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwDirectModifyPrivateRetrievedAttrs);
	dmstate->set_processed = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateSetProcessed));

	/* Create context for per-tuple temp workspace. */
	dmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "postgres_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (dmstate->has_returning)
		dmstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(dmstate->rel));

}

/*
 * postgresIterateDirectModify
 *		Execute a direct foreign table modification
 */
static TupleTableSlot *
odbcIterateDirectModify(ForeignScanState *node)
{
	SQLRETURN 	ret = SQL_SUCCESS;
	odbcFdwDirectModifyState *dmstate = (odbcFdwDirectModifyState *) node->fdw_state;
	EState	   *estate = node->ss.ps.state;
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;

	/*
	 * If this is the first call after Begin, execute the statement.
	 */
	if (dmstate->num_tuples == -1) {
		ret = SQLExecDirect(dmstate->stmt, (SQLCHAR *) dmstate->query, SQL_NTS);
		check_return(ret, "Executing ODBC SQLExecute", dmstate->stmt, SQL_HANDLE_STMT);

		/* Check number of rows affected, and fetch RETURNING tuple if any */
		ret = SQLRowCount(dmstate->stmt, (SQLLEN*)(&dmstate->num_tuples));
		check_return(ret, "Executing ODBC SQLRowCount", dmstate->stmt, SQL_HANDLE_STMT);

	}

	/*
	 * If the local query doesn't specify RETURNING, just clear tuple slot.
	 */
	if (!resultRelInfo->ri_projectReturning)
	{
		TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
		Instrumentation *instr = node->ss.ps.instrument;

		Assert(!dmstate->has_returning);

		/* Increment the command es_processed count if necessary. */
		if (dmstate->set_processed)
			estate->es_processed += dmstate->num_tuples;

		/* Increment the tuple count for EXPLAIN ANALYZE if necessary. */
		if (instr)
			instr->tuplecount += dmstate->num_tuples;

		return ExecClearTuple(slot);
	} else {
		elog(ERROR, "Does not support local query  specified RETURNING");
		return NULL;
	}

}

/*
 * odbcEndDirectModify
 *		Finish a direct foreign table modification
 */
static void
odbcEndDirectModify(ForeignScanState *node)
{
	odbcFdwDirectModifyState *dmstate = (odbcFdwDirectModifyState *) node->fdw_state;

	/* Release remote connection */
	if (dmstate)
	{
		if (dmstate->stmt) 
		{
			SQLFreeHandle(SQL_HANDLE_STMT, dmstate->stmt);
			dmstate->stmt = NULL;
		}

		if (dmstate->conn)
		{
			SQLDisconnect(dmstate->conn);
			SQLFreeHandle(SQL_HANDLE_DBC, dmstate->conn);
			dmstate->conn = NULL;
		}

	}
	/* MemoryContext will be deleted automatically. */
}


/*
 * odbcPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 */
static List *
odbcPlanForeignModify(PlannerInfo *root,
					  ModifyTable *plan,
					  Index resultRelation,
					  int subplan_index)
{
	Relation	rel;
	StringInfoData sql;

	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;
	bool		doNothing = false;

	if (operation != CMD_INSERT) {
		elog(ERROR, "unexpected operation: %d", (int) operation);
		return NULL;
	}

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
#ifndef DIRECT_INSERT
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
#else
	{
		Query	   *parse = root->parse;
		ListCell   *lc;
		foreach(lc, parse->targetList)
		{
			TargetEntry *tle = (TargetEntry*) lfirst(lc);
			targetAttrs = lappend_int(targetAttrs, tle->resno);
		}
	}
#endif
	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
			 (int) plan->onConflictAction);

	/*
	 * Construct the SQL command string.
	 */
	odbc_deparseInsertSql(&sql, root, resultRelation, rel,
					targetAttrs, doNothing, returningList,
					&retrieved_attrs);

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
					  targetAttrs,
					  makeInteger((retrieved_attrs != NIL)),
					  retrieved_attrs);
}

/*
 * postgresExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
odbcExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	elog(ERROR, "should not be here, TBD");
	return  NULL;
}

/*
 * postgresExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *odbcExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	elog(ERROR, "should not be here, TBD");
	return  NULL;
}
/*
 * odbcBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
odbcBeginForeignModify(ModifyTableState *mtstate,
					   ResultRelInfo *resultRelInfo,
					   List *fdw_private,
					   int subplan_index,
					   int eflags)
{
	SQLHDBC dbc;
	SQLHSTMT stmt;
#ifdef DEBUG
	char dsn[256];
	char desc[256];
	SQLSMALLINT dsn_ret;
	SQLSMALLINT desc_ret;
	SQLUSMALLINT direction;
#endif

	odbcFdwOptions options;
	odbcFdwModifyState *fmstate;
	AttrNumber	n_params;
	Oid			typefnoid;
	bool		isvarlena;
	Oid 		typIOParam;
	int32		typemode;
	ListCell   *lc;

	EState	   *estate = mtstate->ps.state;
	CmdType		operation = mtstate->operation;
	Relation	rel = resultRelInfo->ri_RelationDesc;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing odbcFdwModifyState. */
	fmstate = (odbcFdwModifyState *) palloc0(sizeof(odbcFdwModifyState));
	fmstate->rel = rel;
	//-------------------

	/* Fetch the foreign table options */
	odbcGetTableOptions(RelationGetRelid(rel), &options);

	odbc_connection(&options, &dbc);
	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

	//-------------------
	fmstate->conn = dbc;
	fmstate->stmt = stmt;
	fmstate->prepared = false;

	/* Deconstruct fdw_private data. */
	fmstate->query = strVal(list_nth(fdw_private,
									 FdwModifyPrivateUpdateSql));
	fmstate->target_attrs = (List *) list_nth(fdw_private,
											  FdwModifyPrivateTargetAttnums);
	fmstate->has_returning = intVal(list_nth(fdw_private,
											 FdwModifyPrivateHasReturning));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private,
												 FdwModifyPrivateRetrievedAttrs);

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "postgres_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_inputflinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_input_typIOParam = (Oid *) palloc0(sizeof(Oid) * n_params);
	fmstate->p_input_typmod = (int32 *) palloc0(sizeof(int32) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];

			Assert(!attr->attisdropped);

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			getTypeInputInfo(attr->atttypid, &typefnoid, &typIOParam);
			fmgr_info(typefnoid, &fmstate->p_inputflinfo[fmstate->p_nums]);
			typemode = 0;
			getBaseTypeAndTypmod(attr->atttypid, &typemode);
			fmstate->p_input_typIOParam[fmstate->p_nums] = typIOParam;
			fmstate->p_input_typmod[fmstate->p_nums] = typemode;
			fmstate->p_nums++;
		}
	}

	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * odbc_prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
odbc_prepare_foreign_modify(odbcFdwModifyState *fmstate)
{

	SQLRETURN 	ret;
	ret = SQLPrepare(fmstate->stmt, (SQLCHAR *)(fmstate->query), SQL_NTS);
	check_return(ret, "Executing ODBC SQLPrepare", fmstate->stmt, SQL_HANDLE_STMT);

}

/*
 * convert_prep_stmt_params
 *		Create array of text strings representing parameter values
 *
 * tupleid is ctid to send, or NULL if none
 * slot is slot to get remaining parameters from, or NULL if none
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
odbc_convert_prep_stmt_params(odbcFdwModifyState *fmstate,
						 	  ItemPointer tupleid,
							  TupleTableSlot *slot)
{
	const char **p_values;
	int			pindex = 0;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	p_values = (const char **) palloc(sizeof(char *) * fmstate->p_nums);

	/* 1st parameter should be ctid, if it's in use */
	if (tupleid != NULL)
	{
		/* don't need set_transmission_modes for TID output */
		p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
											  PointerGetDatum(tupleid));
		pindex++;
	}

	/* get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		int			nestlevel;
		ListCell   *lc;

		nestlevel = odbc_set_transmission_modes();

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Datum		value;
			bool		isnull;

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				p_values[pindex] = NULL;
			else
				p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
													  value);
			pindex++;
		}

		odbc_reset_transmission_modes(nestlevel);
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}

/**
 * Try to bind parameters to prepared stmt
 */
static void
odbc_Bind_Prepared(odbcFdwModifyState *fmstate, char** p_value)
{
	ListCell   *lc;
    int         attnum;
    Form_pg_attribute   attr;
	int         nums = 0;
	SQLRETURN 	ret = SQL_SUCCESS;
    Datum       result = 0;
    SQLLEN      param_size = 0;
	SQLLEN      column_size = 0;
    char        *param_p = NULL;
    int         dst_type;

	foreach(lc, fmstate->target_attrs)
	{
		attnum = lfirst_int(lc);
		attr = RelationGetDescr(fmstate->rel)->attrs[attnum - 1];
		Assert(!attr->attisdropped);

        if (attr->atttypid == 17 || attr->atttypid == 16) {
		    result = InputFunctionCall(&fmstate->p_inputflinfo[nums], 
                                    	p_value[nums],
										fmstate->p_input_typIOParam[nums],
										fmstate->p_input_typmod[nums]);
	    } else {
            result = (Datum)p_value[nums];
        }

		if (attr->atttypid == 16) {
				// boolean  to SQL_BIT
				SQLSMALLINT odbc_param = (SQLSMALLINT) result ;
				param_size = sizeof(odbc_param);
				ret = SQLBindParameter(fmstate->stmt, nums+1, 
										SQL_PARAM_INPUT, SQL_C_SHORT, 
										SQL_SMALLINT, 0, 0, 
										&odbc_param, 0, &param_size);
		} else if (attr->atttypid == 17) {
				// bytea to SQL_LONGVARBINARY 
				bytea   *string = DatumGetByteaPP(result);
				char    *odbc_param = VARDATA_ANY(string);
				param_size = VARSIZE_ANY_EXHDR(string);
				ret = SQLBindParameter(fmstate->stmt, nums+1, 
										SQL_PARAM_INPUT, SQL_C_CHAR, 
										SQL_LONGVARBINARY, 0, 0, 
										odbc_param, 0, &param_size);
        } else {
		    param_p = (char*)result;
		    switch (attr->atttypid) {
			    case 18:
			    {
				    // char to SQL_CHAR
				    param_size = sizeof(SQLCHAR);
                    dst_type = SQL_CHAR;
				    break;
			    }
			    case 1042: 
			    {
				    //bpchar, char(%u) to SQL_WCHAR
				    param_size = strlen(param_p);
                    dst_type = SQL_WCHAR;
				    break;
			    }
			    case 1043:
			    {
				    //varcar in utf8, to  SQL_VARCHAR
				    param_size = strlen(param_p);
                    dst_type = SQL_VARCHAR;
				    break;
			    }
			    case 25:
			    {
				    //text to SQL_LONGVARCHAR TBD
                    param_size = strlen(param_p);
                    dst_type = SQL_LONGVARCHAR;
				    break;
			    }
			    case 1700: 
			    {
				    // decimal,numerical to	SQL_DECIMAL
				    param_size = strlen(param_p);
                    dst_type = SQL_DECIMAL;
				    break;
			    }
			    case 23:
			    {
				    // int4, integer to	SQL_INTEGER
				    param_size = strlen(param_p);
                    dst_type = SQL_INTEGER;
			    	break;
			    }
			    case 700:
			    {
				    // float4, real to SQL_FLOAT
				    param_size = strlen(param_p);
                    dst_type = SQL_FLOAT;
				    break;
			    }
			    case 701 :
			    {
				    // float8 to SQL_DOUBLE 
				    param_size = strlen(param_p);
                    dst_type = SQL_DOUBLE;
				    break;
			    }
			    case 21:
			    {
				    // int2, smallint to SQL_SMALLINT
                    param_size = strlen(param_p);
                    dst_type = SQL_SMALLINT;
				    break;
			    }
			    case 20:
			    {
				    // int8, bigint to SQL_BIGINT
				    param_size = strlen(param_p);
                    dst_type = SQL_BIGINT;

				    break;
			    }
			    case 1082:
			    {
			    	// date to SQL_DATE
				    param_size = strlen(param_p);
                    dst_type = SQL_DATE;
				    break;
			    }
			    case 1083:
			    {
				    // time to SQL_TIME
				    param_size = strlen(param_p);
                    dst_type = SQL_TIME;
				    break;
			    }
			    case 1114:
			    {
				    //timestamp to SQL_TIMESTAMP
				    param_size = strlen(param_p);
                    dst_type = SQL_TIMESTAMP;
				    break;
			    }
			    case 2950:
			    {
				    // uuid to SQL_GUID
				    param_size = strlen(param_p);
                    dst_type = SQL_GUID;
				    break;
			    }
			    default:
			    {
				    elog(ERROR, "Does not support data type %d", attr->atttypid);
				    break;
			    }
            }

			if (dst_type == SQL_CHAR || dst_type == SQL_WCHAR ||
				dst_type == SQL_VARCHAR || dst_type == SQL_LONGVARCHAR) {
				column_size = param_size;
			} else {
				column_size = 0;
			}
			ret = SQLBindParameter(fmstate->stmt, nums+1, SQL_PARAM_INPUT, 
									SQL_C_CHAR, dst_type, column_size, 0, 
									(SQLCHAR*)param_p, 0, &param_size);

		}
		check_return(ret, "Executing ODBC SQLBindParameter", 
					 fmstate->stmt, SQL_HANDLE_STMT);
		nums++;
	}
	return ;
}

#ifdef DIRECT_INSERT
char *build_insert_sql(odbcFdwModifyState *fmstate, char** p_value)
{
	ListCell			*lc;
	int         		attnum;
	Form_pg_attribute   attr;
	StringInfoData  	buf;
	int					nums = 0;
	char				*param_p = NULL;
	Datum				result = 0;

	initStringInfo(&buf);
	appendStringInfoString(&buf, fmstate->query);
	appendStringInfoChar(&buf, '(');

	foreach(lc, fmstate->target_attrs)
	{
		if (nums >0)
			appendStringInfoChar(&buf, ',');

		attnum = lfirst_int(lc);
		attr = RelationGetDescr(fmstate->rel)->attrs[attnum - 1];
		Assert(!attr->attisdropped);

		if ( attr->atttypid == 16) {
			result = InputFunctionCall(&fmstate->p_inputflinfo[nums],
							p_value[nums],
							fmstate->p_input_typIOParam[nums],
							fmstate->p_input_typmod[nums]);
		} else {
			result = (Datum)p_value[nums];
		}

		if (attr->atttypid == 16) {
			 // boolean  to SQL_BIT
			SQLSMALLINT odbc_param = (SQLSMALLINT) result ;
			if (odbc_param ==0) {
 				appendStringInfoChar(&buf, '0');
			} else {
				appendStringInfoChar(&buf, '1');
            		}
		} else {
			param_p = (char*)result;
			switch (attr->atttypid) {
				case 18:
				{
  				// char to SQL_CHAR
					appendStringInfoChar(&buf, '\'');
					appendStringInfoChar(&buf, *param_p);
					appendStringInfoChar(&buf, '\'');
					break;
				}
				case 17: //// bytea to SQL_LONGVARBINARY 
				case 1042:  //bpchar
				case 1043:  //varchar
				case 1082:// date to SQL_DATE
				case 1083:// time to SQL_TIME
				case 1114://timestamp to SQL_TIMESTAMP
				case 25: //text to SQL_LONGVARCHAR TBD
				{
					appendStringInfoChar(&buf, '\'');
					appendStringInfoString(&buf, param_p);
					appendStringInfoChar(&buf, '\'');
					break;
				}
				case 1700:// decimal,numerical to SQL_DECIMAL
				case 23:// int4, integer to SQL_INTEGER
				case 700:// float4, real to SQL_FLOAT
				case 701:// float8 to SQL_DOUBLE 
				case 21: // int2, smallint to SQL_SMALLINT
				case 20: // int8, bigint to SQL_BIGINT
				case 2950: // uuid to SQL_GUID 
				{
					appendStringInfoString(&buf, param_p);
					break;
				}
    				default:
				{
					elog(ERROR, "Does not support data type %d", attr->atttypid);
					break;
				}
			}
		}
		nums++;
	}
	appendStringInfoChar(&buf, ')');
	return buf.data;
}
#endif

/*
 * odbcExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
odbcExecForeignInsert(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  TupleTableSlot *planSlot)
{
	char **p_values;
	int			n_rows;
	SQLRETURN 	ret;
	SQLLEN		count;
#ifdef DIRECT_INSERT
	char 		*insert_sql;
#endif
	odbcFdwModifyState *fmstate = (odbcFdwModifyState *) resultRelInfo->ri_FdwState;

	/* Set up the prepared statement on the remote server, if we didn't yet */
#ifndef DIRECT_INSERT
	if (!fmstate->prepared) {
		odbc_prepare_foreign_modify(fmstate);
		fmstate->prepared = true;
	}
#endif
	/* Convert parameters needed by prepared statement to text form */
	p_values = (char**) odbc_convert_prep_stmt_params(fmstate, NULL, slot);

	/*
	 * Execute the prepared statement.
	 */
#ifndef	DIRECT_INSERT
	odbc_Bind_Prepared(fmstate, p_values);

	/*
	 * Get the result, and check for success.
	 *
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	ret = SQLExecute(fmstate->stmt);
#else
	insert_sql = build_insert_sql(fmstate, p_values);
	ret = SQLExecDirect(fmstate->stmt, (SQLCHAR *) insert_sql, SQL_NTS);
#endif

	check_return(ret, "Executing ODBC SQLExecute", fmstate->stmt, SQL_HANDLE_STMT);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	ret = SQLRowCount(fmstate->stmt, &count);
	check_return(ret, "Executing ODBC SQLRowCount", fmstate->stmt, SQL_HANDLE_STMT);

	n_rows = count;
	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * odbcEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
odbcEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	odbcFdwModifyState *fmstate = (odbcFdwModifyState *) resultRelInfo->ri_FdwState;

	/* Release remote connection */
	if (fmstate)
	{	
		if (fmstate->stmt)
		{
			SQLFreeHandle(SQL_HANDLE_STMT, fmstate->stmt);
			fmstate->stmt = NULL;
		}

		if (fmstate->conn)
		{
			SQLDisconnect(fmstate->conn);
			SQLFreeHandle(SQL_HANDLE_DBC, fmstate->conn);
			fmstate->conn = NULL;
		}

	}
}

/*
 * odbcIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
odbcIsForeignRelUpdatable(Relation rel)
{
	bool		updatable;
	ForeignTable *table;
	ForeignServer *server;
	ListCell   *lc;

	/*
	 * By default, all postgres_fdw foreign tables are assumed updatable. This
	 * can be overridden by a per-server setting, which in turn can be
	 * overridden by a per-table setting.
	 */
	updatable = true;

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0) {
			updatable = defGetBoolean(def);
			break;
		}
	}

	if (!updatable) {
		foreach(lc, table->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "updatable") == 0) {
				updatable = defGetBoolean(def);
				break;
			}
		}
	}

	/*
	 * Currently "updatable" means support for INSERT, UPDATE and DELETE.
	 */
	return updatable ?
		(1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}

List *
odbcImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	/* TODO: review memory management in this function; any leaks? */
	odbcFdwOptions options;

	List* create_statements = NIL;
	List* tables = NIL;
	List* table_columns = NIL;
	ListCell *tables_cell;
	ListCell *table_columns_cell;
	RangeVar *table_rangevar;

	SQLHDBC dbc;
	SQLHSTMT query_stmt;
	SQLHSTMT columns_stmt;
	SQLHSTMT tables_stmt;
	SQLRETURN ret;
	SQLSMALLINT result_columns;
	StringInfoData col_str;
	SQLCHAR *ColumnName;
	SQLCHAR *TableName;
	SQLSMALLINT NameLength;
	SQLSMALLINT DataType;
	SQLULEN     ColumnSize;
	SQLSMALLINT DecimalDigits;
	SQLSMALLINT Nullable;
	int i;
	StringInfoData sql_type;
	SQLLEN indicator;
	const char* schema_name;
	bool missing_foreign_schema = false;

	elog_debug("%s", __func__);

	odbcGetOptions(serverOid, stmt->options, &options);

	schema_name = get_schema_name(&options);
	if (schema_name == NULL)
	{
		schema_name = stmt->remote_schema;
		missing_foreign_schema = true;
	}
	else if (is_blank_string(schema_name))
	{
		// This allows overriding and removing the schema, which is necessary
		// for some schema-less ODBC data sources (e.g. Hive)
		schema_name = NULL;
	}

	if (!is_blank_string(options.sql_query))
	{
		/* Generate foreign table for a query */
		if (is_blank_string(options.table))
		{
			elog(ERROR, "Must provide 'table' option to name the foreign table");
		}

		odbc_connection(&options, &dbc);

		/* Allocate a statement handle */
		SQLAllocHandle(SQL_HANDLE_STMT, dbc, &query_stmt);

		/* Retrieve a list of rows */
		ret = SQLExecDirect(query_stmt, (SQLCHAR *) options.sql_query, SQL_NTS);
		check_return(ret, "Executing ODBC query", query_stmt, SQL_HANDLE_STMT);

		SQLNumResultCols(query_stmt, &result_columns);

		initStringInfo(&col_str);
		ColumnName = (SQLCHAR *) palloc(sizeof(SQLCHAR) * MAXIMUM_COLUMN_NAME_LEN);

		for (i = 1; i <= result_columns; i++)
		{
			SQLDescribeCol(query_stmt,
			               i,                       /* ColumnName */
			               ColumnName,
			               sizeof(SQLCHAR) * MAXIMUM_COLUMN_NAME_LEN, /* BufferLength */
			               &NameLength,
			               &DataType,
			               &ColumnSize,
			               &DecimalDigits,
			               &Nullable);

			sql_data_type(DataType, ColumnSize, DecimalDigits, Nullable, &sql_type);
			if (is_blank_string(sql_type.data))
			{
				elog(NOTICE, "Data type not supported (%d) for column %s", DataType, ColumnName);
				continue;
			}
			if (i > 1)
			{
				appendStringInfo(&col_str, ", ");
			}
			appendStringInfo(&col_str, "\"%s\" %s", ColumnName, (char *) sql_type.data);
		}
		SQLCloseCursor(query_stmt);
		SQLFreeHandle(SQL_HANDLE_STMT, query_stmt);

		tables        = lappend(tables, (void*)options.table);
		table_columns = lappend(table_columns, (void*)col_str.data);
	}
	else
	{
		/* Reflect one or more foreign tables */
		if (!is_blank_string(options.table))
		{
			tables = lappend(tables, (void*)options.table);
		}
		else if (stmt->list_type == FDW_IMPORT_SCHEMA_ALL ||
				 stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			/* Will obtain the foreign tables with SQLTables() */

			SQLCHAR *table_schema = (SQLCHAR *) palloc(sizeof(SQLCHAR) *
															  MAXIMUM_SCHEMA_NAME_LEN);

			odbc_connection(&options, &dbc);

			/* Allocate a statement handle */
			SQLAllocHandle(SQL_HANDLE_STMT, dbc, &tables_stmt);

			ret = SQLTables(
			          tables_stmt,
			          NULL, 0, /* Catalog: (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS 
								would include also tables from internal catalogs */
			          NULL, 0, /* Schema: we avoid filtering by schema here to 
								avoid problems with some drivers */
			          NULL, 0, /* Table */
			          (SQLCHAR*)"TABLE", SQL_NTS /* Type of table (we're not 
									interested in views, temporary tables, etc.) */
			      );
			check_return(ret, "Obtaining ODBC tables", tables_stmt, SQL_HANDLE_STMT);

			initStringInfo(&col_str);
			while (SQL_SUCCESS == ret)
			{
				ret = SQLFetch(tables_stmt);
				if (SQL_SUCCESS == ret)
				{
					int excluded = false;
					TableName = (SQLCHAR *) palloc(sizeof(SQLCHAR) *
														  MAXIMUM_TABLE_NAME_LEN);
					ret = SQLGetData(tables_stmt, SQLTABLES_NAME_COLUMN,
									 SQL_C_CHAR, TableName, 
									 MAXIMUM_TABLE_NAME_LEN, &indicator);
					check_return(ret, "Reading table name", tables_stmt, 
								 SQL_HANDLE_STMT);

					/* Since we're not filtering the SQLTables call by schema
					   we must exclude here tables that belong to other schemas.
					   For some ODBC drivers tables may not be organized into
					   schemas and the schema of the table will be blank.
					   So we only reject tables for which the schema is not
					   blank and different from the desired schema:
					 */
					ret = SQLGetData(tables_stmt, SQLTABLES_SCHEMA_COLUMN, 
									 SQL_C_CHAR, table_schema, 
									 MAXIMUM_SCHEMA_NAME_LEN, &indicator);
					if (SQL_SUCCESS == ret)
					{
						if (!is_blank_string((char*)table_schema) &&
							strcmp((char*)table_schema, schema_name) )
						{
							excluded = true;
						}
					}
					else
					{
						/* Some drivers don't support schemas and may return an error code here;
						 * in that case we must avoid using an schema to query the table columns.
						 */
						schema_name = NULL;
					}

					/* Since we haven't specified SQL_ALL_CATALOGS in the
					   call to SQLTables we shouldn't get tables from special
					   catalogs and only from the regular catalog of the database
					   (the catalog name is usually the name of the database or blank,
					   but depends on the driver and may vary, and can be obtained with:
					     SQLCHAR *table_catalog = (SQLCHAR *) palloc(sizeof(SQLCHAR) * MAXIMUM_CATALOG_NAME_LEN);
					     SQLGetData(tables_stmt, 1, SQL_C_CHAR, table_catalog, MAXIMUM_CATALOG_NAME_LEN, &indicator);
					 */

					/* And now we'll handle tables excluded by an EXCEPT clause */
					if (!excluded && stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
					{
						foreach(tables_cell,  stmt->table_list)
						{
							table_rangevar = (RangeVar*)lfirst(tables_cell);
							if (strcmp((char*)TableName, table_rangevar->relname) == 0)
							{
								excluded = true;
							}
						}
					}

					if (!excluded)
					{
						tables = lappend(tables, (void*)TableName);
					}
				}
			}

			SQLCloseCursor(tables_stmt);

			SQLFreeHandle(SQL_HANDLE_STMT, tables_stmt);
		}
		else if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
		{
			foreach(tables_cell, stmt->table_list)
			{
				table_rangevar = (RangeVar*)lfirst(tables_cell);
				tables = lappend(tables, (void*)table_rangevar->relname);
			}
		}
		else
		{
			elog(ERROR,"Unknown list type in IMPORT FOREIGN SCHEMA");
		}

		foreach(tables_cell, tables)
		{
			char *table_name = (char*)lfirst(tables_cell);

			odbc_connection(&options, &dbc);

			/* Allocate a statement handle */
			SQLAllocHandle(SQL_HANDLE_STMT, dbc, &columns_stmt);

			ret = SQLColumns(
			          columns_stmt,
			          NULL, 0,
			          (SQLCHAR*)schema_name, SQL_NTS,
			          (SQLCHAR*)table_name,  SQL_NTS,
			          NULL, 0
			      );
			check_return(ret, "Obtaining ODBC columns", columns_stmt, SQL_HANDLE_STMT);

			i = 0;
			initStringInfo(&col_str);
			ColumnName = (SQLCHAR *) palloc(sizeof(SQLCHAR) * MAXIMUM_COLUMN_NAME_LEN);
			while (SQL_SUCCESS == ret)
			{
				ret = SQLFetch(columns_stmt);
				if (SQL_SUCCESS == ret)
				{
					ret = SQLGetData(columns_stmt, 4, SQL_C_CHAR, ColumnName,
									 MAXIMUM_COLUMN_NAME_LEN, &indicator);
					// check_return(ret, "Reading column name", columns_stmt, SQL_HANDLE_STMT);
					ret = SQLGetData(columns_stmt, 5, SQL_C_SSHORT, &DataType, 
									 MAXIMUM_COLUMN_NAME_LEN, &indicator);
					// check_return(ret, "Reading column type", columns_stmt, SQL_HANDLE_STMT);
					ret = SQLGetData(columns_stmt, 7, SQL_C_SLONG, &ColumnSize, 
									 0, &indicator);
					// check_return(ret, "Reading column size", columns_stmt, SQL_HANDLE_STMT);
					ret = SQLGetData(columns_stmt, 9, SQL_C_SSHORT, &DecimalDigits, 
									 0, &indicator);
					// check_return(ret, "Reading column decimals", columns_stmt, SQL_HANDLE_STMT);
					ret = SQLGetData(columns_stmt, 11, SQL_C_SSHORT, &Nullable, 
									 0, &indicator);
					// check_return(ret, "Reading column nullable", columns_stmt, SQL_HANDLE_STMT);
					sql_data_type(DataType, ColumnSize, DecimalDigits, Nullable,
								  &sql_type);
					if (is_blank_string(sql_type.data))
					{
						elog(NOTICE, "Data type not supported (%d) for column %s", 
							 		 DataType, ColumnName);
						continue;
					}
					if (++i > 1)
					{
						appendStringInfo(&col_str, ", ");
					}
					appendStringInfo(&col_str, "\"%s\" %s", 
												ColumnName, (char *) sql_type.data);
				}
			}
			SQLCloseCursor(columns_stmt);
			SQLFreeHandle(SQL_HANDLE_STMT, columns_stmt);
			table_columns = lappend(table_columns, (void*)col_str.data);
		}
	}

	/* Generate create statements */
	table_columns_cell = list_head(table_columns);
	foreach(tables_cell, tables)
	{
		// temporarily define vars here...
		char *table_name = (char*)lfirst(tables_cell);
		char *columns    = (char*)lfirst(table_columns_cell);
		StringInfoData create_statement;
		ListCell *option;
		int option_count = 0;
		const char *prefix = empty_string_if_null(options.prefix);

		table_columns_cell = lnext(table_columns_cell);

		initStringInfo(&create_statement);
		appendStringInfo(&create_statement, 
						 "CREATE FOREIGN TABLE \"%s\".\"%s%s\" (", 
						 stmt->local_schema, prefix, (char *) table_name);
		appendStringInfo(&create_statement, "%s", columns);
		appendStringInfo(&create_statement, ") SERVER %s\n", stmt->server_name);
		appendStringInfo(&create_statement, "OPTIONS (\n");
		foreach(option, stmt->options)
		{
			DefElem *def = (DefElem *) lfirst(option);
			appendOption(&create_statement, ++option_count == 1, 
						 def->defname, defGetString(def));
		}
		if (is_blank_string(options.table))
		{
			appendOption(&create_statement, ++option_count == 1, "table", table_name);
		}
		if (missing_foreign_schema)
		{
			appendOption(&create_statement, ++option_count == 1, "schema", schema_name);
		}
		appendStringInfo(&create_statement, ");");
		elog(DEBUG1, "CREATE: %s", create_statement.data);
		create_statements = lappend(create_statements, (void*)create_statement.data);
	}

	return create_statements;
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
odbc_set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
odbc_reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
extern Expr *
odbc_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_is_subset(em->em_relids, rel->relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
	Expr	   *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								Path *epq_path)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		estimate_path_cost_size(root, rel, NIL, useful_pathkeys,
								&rows, &width, &startup_cost, &total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself,
		 * in case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		add_path(rel, (Path *)
				 create_foreignscan_path(root, rel,
										 NULL,
										 rows,
										 startup_cost,
										 total_cost,
										 useful_pathkeys,
										 NULL,
										 sorted_epq_path,
										 NIL));
	}
}

static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		cpu_per_tuple;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */

	{
		Cost		run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations and set_joinrel_size_estimates() for join
		 * between foreign relations.
		 */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/* Back into an estimate of the number of retrieved rows. */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

		/*
		 * We will come here again and again with different set of pathkeys
		 * that caller wants to cost. We don't need to calculate the cost of
		 * bare scan each time. Instead, use the costs if we have cached them
		 * already.
		 */
		if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
		{
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		}
		else if (IS_JOIN_REL(foreignrel))
		{
			PgFdwRelationInfo *fpinfo_i;
			PgFdwRelationInfo *fpinfo_o;
			QualCost	join_cost;
			QualCost	remote_conds_cost;
			double		nrows;

			/* For join we expect inner and outer relations set */
			Assert(fpinfo->innerrel && fpinfo->outerrel);

			fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
			fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;

			/* Estimate of number of rows in cross product */
			nrows = fpinfo_i->rows * fpinfo_o->rows;
			/* Clamp retrieved rows estimate to at most size of cross product */
			retrieved_rows = Min(retrieved_rows, nrows);

			/*
			 * The cost of foreign join is estimated as cost of generating
			 * rows for the joining relations + cost for applying quals on the
			 * rows.
			 */

			/*
			 * Calculate the cost of clauses pushed down to the foreign server
			 */
			cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
			/* Calculate the cost of applying join clauses */
			cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

			/*
			 * Startup cost includes startup cost of joining relations and the
			 * startup cost for join and other clauses. We do not include the
			 * startup cost specific to join strategy (e.g. setting up hash
			 * tables) since we do not know what strategy the foreign server
			 * is going to use.
			 */
			startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
			startup_cost += join_cost.startup;
			startup_cost += remote_conds_cost.startup;
			startup_cost += fpinfo->local_conds_cost.startup;

			/*
			 * Run time cost includes:
			 *
			 * 1. Run time cost (total_cost - startup_cost) of relations being
			 * joined
			 *
			 * 2. Run time cost of applying join clauses on the cross product
			 * of the joining relations.
			 *
			 * 3. Run time cost of applying pushed down other clauses on the
			 * result of join
			 *
			 * 4. Run time cost of applying nonpushable other clauses locally
			 * on the result fetched from the foreign server.
			 */
			run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
			run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
			run_cost += nrows * join_cost.per_tuple;
			nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
			run_cost += nrows * remote_conds_cost.per_tuple;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		}
		else if (IS_UPPER_REL(foreignrel))
		{
			PgFdwRelationInfo *ofpinfo;
			PathTarget *ptarget = root->upper_targets[UPPERREL_GROUP_AGG];
			AggClauseCosts aggcosts;
			double		input_rows;
			int			numGroupCols;
			double		numGroups = 1;

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 *
			 * Also, core does not care about costing HAVING expressions and
			 * adding that to the costs.  So similarly, here too we are not
			 * considering remote and local conditions for costing.
			 */

			ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;

			/* Get rows and width from input rel */
			input_rows = ofpinfo->rows;
			width = ofpinfo->width;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
				get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE, &aggcosts);
				get_agg_clause_costs(root, (Node *) root->parse->havingQual,
									 AGGSPLIT_SIMPLE, &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
											get_sortgrouplist_exprs(root->parse->groupClause,
																	fpinfo->grouped_tlist),
											input_rows, NULL);

			/*
			 * Number of rows expected from foreign server will be same as
			 * that of number of groups.
			 */
			rows = retrieved_rows = numGroups;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input * relation
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *	  3. Startup cost for PathTarget eval
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;
			startup_cost += ptarget->cost.startup;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *	  3. PathTarget eval cost for each output row
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += aggcosts.finalCost * numGroups;
			run_cost += cpu_tuple_cost * numGroups;
			run_cost += ptarget->cost.per_tuple * numGroups;
		}
		else
		{
			/* Clamp retrieved rows estimates to at most foreignrel->tuples. */
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}

		total_cost = startup_cost + run_cost;
	}

	/*
	 * Cache the costs for scans without any pathkeys or parameterization
	 * before adding the costs for transferring data from the foreign server.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * foreign server. This function will be called at least once for every
	 * foreign relation without pathkeys and parameterization.
	 */
	if (pathkeys == NIL && param_join_conds == NIL)
	{
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr	   *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * odbc_is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile ||
				!(em_expr = odbc_find_em_expr_for_rel(pathkey_ec, rel)) ||
				!odbc_is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		Expr	   *em_expr;
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* If no pushable expression for this rel, skip it. */
		em_expr = odbc_find_em_expr_for_rel(cur_ec, rel);
		if (em_expr == NULL || !odbc_is_foreign_expr(root, rel, em_expr))
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
#if PG_VERSION_NUM >= 100000
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
#elif  PG_VERSION_NUM >= 90600
		relids = find_childrel_top_parent(root, rel)->relids;
#endif 
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

#if PG_VERSION_NUM >= 100000
/*
 * odbcGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
static void
odbcGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	PgFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((PgFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if (stage != UPPERREL_GROUP_AGG || output_rel->fdw_private)
		return;

	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	output_rel->fdw_private = fpinfo;

	add_foreign_grouping_paths(root, input_rel, output_rel);
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel)
{
	Query	   *parse = root->parse;
	PgFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	PathTarget *grouping_target;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/* Assess if it is safe to push down aggregation and grouping. */
	if (!foreign_grouping_ok(root, grouped_rel))
		return;

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, NIL, &rows,
							&width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										grouping_target,
										rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										NULL,	/* no required_outer */
										NULL,
										NIL);	/* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
{
	Query	   *query = root->parse;
	PathTarget *grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) grouped_rel->fdw_private;
	PgFdwRelationInfo *ofpinfo;
	List	   *aggvars;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!odbc_is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Is it shippable?
			 */
			if (odbc_is_foreign_expr(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.
				 */
				if (!odbc_is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (root->hasHavingQual && query->havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) query->havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
			if (odbc_is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.
			 */
			if (IsA(expr, Aggref))
			{
				if (!odbc_is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs, during one (usually the
	 * first) of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
					 ofpinfo->relation_name->data);

	return true;
}
#endif

static void
merge_fdw_options(PgFdwRelationInfo *fpinfo,
				  const PgFdwRelationInfo *fpinfo_o,
				  const PgFdwRelationInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
			fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);
	}
}
