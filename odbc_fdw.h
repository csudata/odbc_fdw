/*-------------------------------------------------------------------------
 *
 * odbc_fdw.h
 *		  ODBC Foreign-data wrapper for remote database
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2017-2018, www.cstech.ltd

 * IDENTIFICATION
 *		  contrib/odbc_fdw/odbc_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ODBC_FDW_H
#define ODBC_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

#if PG_VERSION_NUM < 100000 && PG_VERSION_NUM >= 90600
#define IS_SIMPLE_REL(rel) \
        ((rel)->reloptkind == RELOPT_BASEREL || \
         (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)

#define IS_OTHER_REL(rel) ((rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
/* Is the given relation a join relation? */
#define IS_JOIN_REL(rel) ((rel)->reloptkind == RELOPT_JOINREL)

/* Is the given relation an upper relation? */
#define IS_UPPER_REL(rel) ((rel)->reloptkind == RELOPT_UPPER_REL)
#endif
/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * postgres_fdw foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 * postgresGetForeignJoinPaths creates it for a joinrel, and
 * postgresGetForeignUpperPaths creates it for an upperrel.
 */
typedef struct PgFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 * All entries in these lists should have RestrictInfo wrappers; that
	 * improves efficiency of selectivity and cost estimation.
	 */
	List	   *remote_conds;
	List	   *local_conds;

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Selectivity of join conditions */
	Selectivity joinclause_sel;

	/* Estimated size and cost for a scan or join. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	/* Costs excluding costs for transferring data from the foreign server */
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	bool		use_remote_estimate;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;			/* only set in use_remote_estimate mode */

	int			fetch_size;		/* fetch size for this remote table */

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo	relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List	   *joinclauses;	/* List of RestrictInfo */

	/* Grouping information */
	List	   *grouped_tlist;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;
} PgFdwRelationInfo;


/* in odbc_deparse.c */
extern void odbc_classifyConditions(PlannerInfo *root,
				   RelOptInfo *baserel,
				   List *input_conds,
				   List **remote_conds,
				   List **local_conds);
extern bool odbc_is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr);
extern void odbc_deparseInsertSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *targetAttrs, bool doNothing, List *returningList,
				 List **retrieved_attrs);
extern void odbc_deparseDirectUpdateSql(StringInfo buf, PlannerInfo *root,
					   Index rtindex, Relation rel,
					   List *targetlist,
					   List *targetAttrs,
					   List *remote_conds,
					   List **params_list,
					   List *returningList,
					   List **retrieved_attrs);
extern void odbc_deparseDeleteSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *returningList,
				 List **retrieved_attrs);
extern void odbc_deparseDirectDeleteSql(StringInfo buf, PlannerInfo *root,
					   Index rtindex, Relation rel,
					   List *remote_conds,
					   List **params_list,
					   List *returningList,
					   List **retrieved_attrs);
extern void odbc_deparseStringLiteral(StringInfo buf, const char *val);
extern Expr *odbc_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern List *odbc_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void odbc_deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root,
						RelOptInfo *foreignrel, List *tlist,
						List *remote_conds, List *pathkeys, bool is_subquery,
						List **retrieved_attrs, List **params_list);
extern const char *odbc_get_jointype_name(JoinType jointype);
enum ShipObj            
{               
	AggObj,
	ProcedureObj,
	DataTypeObj,    
	OperatorObj,
	UNKOWNOBJ               
};
#define SHIPAGGMIN 2100   
#define SHIPAGGMAX 2803 
/* in odbc_shippable.c */
extern bool odbc_is_builtin(Oid objectId);
extern bool odbc_is_shippable(Oid objectId, Oid classId, PgFdwRelationInfo *fpinfo, enum ShipObj ObjType);

/* in odbc_fdw.c */
extern int odbc_set_transmission_modes(void);
extern void odbc_reset_transmission_modes(int nestlevel);
#endif							/* ODBC_FDW_H */
