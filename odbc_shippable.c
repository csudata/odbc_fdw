/*-------------------------------------------------------------------------
 *
 * odbc_shippable.c
 *	  Determine which database objects are shippable to a remote server.
 *
 * We need to determine whether particular functions, operators, and indeed
 * data types are shippable to a remote server for execution --- that is,
 * do they exist and have the same behavior remotely as they do locally?
 * Built-in objects are generally considered shippable.  Other objects can
 * be shipped if they are white-listed by the user.
 *
 * Note: there are additional filter rules that prevent shipping mutable
 * functions or functions using nonportable collations.  Those considerations
 * need not be accounted for here.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 2017-2018, www.cstech.ltd
 * IDENTIFICATION
 *	  contrib/odbc_fdw/odbc_shippable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "odbc_fdw.h"

#include "access/transam.h"
#include "catalog/dependency.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/syscache.h"


/* Hash table for caching the results of shippability lookups */
static HTAB *ShippableCacheHash = NULL;

/*
 * Hash key for shippability lookups.  We include the FDW server OID because
 * decisions may differ per-server.  Otherwise, objects are identified by
 * their (local!) OID and catalog OID.
 */
typedef struct
{
	/* XXX we assume this struct contains no padding bytes */
	Oid			objid;			/* function/operator/type OID */
	Oid			classid;		/* OID of its catalog (pg_proc, etc) */
	Oid			serverid;		/* FDW server we are concerned with */
} ShippableCacheKey;

typedef struct
{
	ShippableCacheKey key;		/* hash key - must be first */
	bool		shippable;
} ShippableCacheEntry;


/*
 * Flush cache entries when pg_foreign_server is updated.
 *
 * We do this because of the possibility of ALTER SERVER being used to change
 * a server's extensions option.  We do not currently bother to check whether
 * objects' extension membership changes once a shippability decision has been
 * made for them, however.
 */
static void
InvalidateShippableCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	ShippableCacheEntry *entry;

	/*
	 * In principle we could flush only cache entries relating to the
	 * pg_foreign_server entry being outdated; but that would be more
	 * complicated, and it's probably not worth the trouble.  So for now, just
	 * flush all entries.
	 */
	hash_seq_init(&status, ShippableCacheHash);
	while ((entry = (ShippableCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(ShippableCacheHash,
						(void *) &entry->key,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

/*
 * Initialize the backend-lifespan cache of shippability decisions.
 */
static void
InitializeShippableCache(void)
{
	HASHCTL		ctl;

	/* Create the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ShippableCacheKey);
	ctl.entrysize = sizeof(ShippableCacheEntry);
	ShippableCacheHash =
		hash_create("Shippability cache", 256, &ctl, HASH_ELEM | HASH_BLOBS);

	/* Set up invalidation callback on pg_foreign_server. */
	CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
								  InvalidateShippableCacheCallback,
								  (Datum) 0);
}

/*
 * Returns true if given object (operator/function/type) is shippable
 * according to the server options.
 *
 * Right now "shippability" is exclusively a function of whether the object
 * belongs to an extension declared by the user.  In the future we could
 * additionally have a whitelist of functions/operators declared one at a time.
 */
static bool
lookup_shippable(Oid objectId, Oid classId, PgFdwRelationInfo *fpinfo)
{
	Oid			extensionOid;

	/*
	 * Is object a member of some extension?  (Note: this is a fairly
	 * expensive lookup, which is why we try to cache the results.)
	 */
	extensionOid = getExtensionOfObject(classId, objectId);

	/* If so, is that extension in fpinfo->shippable_extensions? */
	if (OidIsValid(extensionOid) &&
		list_member_oid(fpinfo->shippable_extensions, extensionOid))
		return true;

	return false;
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else deparse_type_name might incorrectly fail to schema-qualify their names.
 * Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
odbc_is_builtin(Oid objectId)
{
	return (objectId < FirstBootstrapObjectId);
}

/*
 * is_shippable
 *	   Is this object (function/operator/type) shippable to foreign server?
 */
bool
odbc_is_shippable(Oid objectId, Oid classId, PgFdwRelationInfo *fpinfo, enum ShipObj objType)
{
	ShippableCacheKey key;
	ShippableCacheEntry *entry;

	/* Built-in objects are presumed shippable. */
	//if (is_builtin(objectId))
	//	return true;

	if (objType == AggObj) {
		if (objectId >= SHIPAGGMIN && objectId <= SHIPAGGMAX) {
			return true;
		} 
	} else if (objType == DataTypeObj) {
		if (objectId == 16 || objectId == 17 || objectId == 18 ||  objectId == 20 ||
			objectId == 21 || objectId == 23 || objectId == 25 || objectId == 700 ||
			objectId == 701 || objectId == 1042 ||objectId == 1043 ||objectId == 1082 ||
			objectId == 1083 || objectId == 1114 ||objectId == 1700 ||objectId == 2950)
			return true;
	} else if (objType == OperatorObj) {
			return true;
	} else if (objType == ProcedureObj) {
                if ((objectId >= 1242 && objectId <= 1245) ||(objectId >= 31 && objectId <= 39) ||
                        (objectId == 42 || objectId == 43) ||(objectId == 46 || objectId == 47)||
                        (objectId >= 56 && objectId <= 67) ||(objectId >= 70 && objectId <= 84) ||
                        (objectId >= 1252 && objectId <= 1258) ||(objectId >= 141 && objectId <= 183) ||
                        (objectId >= 200 && objectId <= 224) ||(objectId >= 235 && objectId <= 238) ||
                        (objectId >= 1242 && objectId <= 1245) ||(objectId >= 240 && objectId <= 319) ||
                        (objectId >= 350 && objectId <= 382) ||(objectId >= 3129 && objectId <= 3135) ||
                        (objectId >= 438 && objectId <= 483) ||(objectId >= 652 && objectId <= 659) ||
                        (objectId >= 740 && objectId <= 743) ||(objectId >= 766 && objectId <= 848) ||
                        (objectId >= 852 && objectId <= 857) ||(objectId == 3399 || objectId == 3344 || objectId == 3345 ) ||
                        (objectId >= 862 && objectId <= 946) ||(objectId >= 3822 || objectId <= 3824) ||
                        (objectId == 3811 || objectId == 3812) || (objectId >= 1044 && objectId <= 1078) ||objectId == 3328 ||
                        (objectId >= 1084 && objectId <= 1092) ||(objectId >= 1102 && objectId <= 1145) ||objectId == 3136 ||
                        (objectId >= 1150 && objectId <= 1158) ||(objectId == 1195 ||
                        objectId == 1196) || objectId == 1219 || objectId == 3546 ||
                        (objectId >= 1236 && objectId <= 1253) ||(objectId >= 1274 && objectId <= 1281) ||
                        (objectId >= 837 && objectId <= 848) ||(objectId == 1296 || objectId <= 1298) ||
                        (objectId >= 1312 && objectId <= 1316) ||(objectId >= 1350 && objectId <= 1359) ||
                        (objectId >= 1377 && objectId <= 1380) ||(objectId >= 1564 && objectId <= 1572) ||
                        (objectId >= 1579 && objectId <= 1596) ||(objectId >= 1631 && objectId <= 1661) ||
                        (objectId >= 1666 && objectId <= 1672) ||(objectId >= 1691 && objectId <= 1693) ||
                        (objectId >= 1701 && objectId <= 1703) ||(objectId >= 1718 && objectId <= 1728) ||
                        (objectId >= 1742 && objectId <= 1746) ||(objectId >= 1764 && objectId <= 1783) ||objectId == 3283 ||
                        (objectId >= 1850 && objectId <= 1862) ||(objectId >= 1910 && objectId <= 1915) ||
                        (objectId >= 1948 && objectId <= 1954) || objectId == 3331 || (objectId >= 2005 && objectId <=2008) ||
                        (objectId >= 2052 && objectId <= 2057) ||(objectId >= 1910 && objectId <= 1915) ||objectId == 3137 ||
                        (objectId >= 2160 && objectId <= 2195) ||(objectId == 3322 || objectId ==3333) ||
                        (objectId >= 2338 && objectId <= 2383) ||(objectId >= 2520 && objectId <= 2533))

                        return true;
			return true;
	}
	/* Otherwise, give up if user hasn't specified any shippable extensions. */
	if (fpinfo->shippable_extensions == NIL)
		return false;

	/* Initialize cache if first time through. */
	if (!ShippableCacheHash)
		InitializeShippableCache();

	/* Set up cache hash key */
	key.objid = objectId;
	key.classid = classId;
	key.serverid = fpinfo->server->serverid;

	/* See if we already cached the result. */
	entry = (ShippableCacheEntry *)
		hash_search(ShippableCacheHash,
					(void *) &key,
					HASH_FIND,
					NULL);

	if (!entry)
	{
		/* Not found in cache, so perform shippability lookup. */
		bool		shippable = lookup_shippable(objectId, classId, fpinfo);

		/*
		 * Don't create a new hash entry until *after* we have the shippable
		 * result in hand, as the underlying catalog lookups might trigger a
		 * cache invalidation.
		 */
		entry = (ShippableCacheEntry *)
			hash_search(ShippableCacheHash,
						(void *) &key,
						HASH_ENTER,
						NULL);

		entry->shippable = shippable;
	}

	return entry->shippable;
}
