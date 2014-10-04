/*-------------------------------------------------------------------------
 *
 * decode.c
 *		Logical decoding output plugin generating SQL queries based
 *		on things decoded.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Copyright (c) 2014, rektide de la faye
 *
 * IDENTIFICATION
 *		decoder_raw/decoder_raw.c
 *		decode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"


#include "access/genam.h"
#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "putpostlogic.h"

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if standard_conforming_strings were enabled.
 */
static json_object*
print_literal(Oid typid, Oid typoutput, Datum origval)
{
	switch (typid)
	{
		case BOOLOID:
			return json_object_new_boolean(BoolGetDatum(origval));
		case INT2OID:
			return json_object_new_int(Int16GetDatum(origval));
		case INT4OID:
			return json_object_new_int(Int32GetDatum(origval));
		case INT8OID:
			return json_object_new_int64(Int64GetDatumFast(origval));
		case FLOAT4OID:
			return json_object_new_double((double)Float4GetDatum(origval));
		case FLOAT8OID:
			return json_object_new_double(Float8GetDatum(origval));
		case NUMERICOID:
		case OIDOID:
			return json_object_new_string(OidOutputFunctionCall(typoutput, origval));
		case BITOID:
		case VARBITOID:
		default:
			return json_object_new_string(OidOutputFunctionCall(typoutput, origval));
	}
}

/*
 * Return a relation name
 */
static json_object*
print_relname(Relation rel)
{
	Form_pg_class  class_form = RelationGetForm(rel);
	char          *identifier = quote_qualified_identifier(
	                             get_namespace_name(
	                              get_rel_namespace(RelationGetRelid(rel))),
	                             NameStr(class_form->relname));
	return json_object_new_string(identifier);
}

/*
 * Return a value
 */
static json_object*
print_value(Datum origval, Oid typid, bool isnull)
{
	json_object *json;
	Oid          typoutput;
	bool         typisvarlena;

	// Query output function
	getTypeOutputInfo(typid, &typoutput, &typisvarlena);

	// Print value
	if (isnull)
	{
	}
	else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
	{
		json = json_object_new_string("~~unchanged-toast-datum~~");
	}
	else
	{
		if (typisvarlena)
		{
			// Definitely detoasted Datum
			origval = PointerGetDatum(PG_DETOAST_DATUM(origval));
		}

		//o = print_literal(typid, OidOutputFunctionCall(typoutput, origval));
		json = print_literal(typid, typoutput, origval);
	}
	return json;
}

/*
 * Print a WHERE clause item
 */
static void
print_where_clause_item(json_object *json,
						Relation relation,
						HeapTuple tuple,
						int natt)
{
	Form_pg_attribute	attr;
	Datum				origval;
	bool				isnull;
	TupleDesc			tupdesc = RelationGetDescr(relation);
	const char         *name;
	json_object        *val;

	attr = tupdesc->attrs[natt];

	// Skip dropped columns and system columns
	if (attr->attisdropped || attr->attnum < 0)
		return;

	// Print attribute name
	name = quote_identifier(NameStr(attr->attname));

	// Get Datum from tuple
	origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

	// Get output function
	val = print_value(origval, attr->atttypid, isnull);
	json_object_object_add(json, name, val);
}

/*
 * Generate a WHERE clause for UPDATE or DELETE.
 */
static void
print_where_clause(json_object *json,
				   Relation relation,
				   HeapTuple oldtuple,
				   HeapTuple newtuple)
{
	TupleDesc		tupdesc = RelationGetDescr(relation);
	int				natt;

	Assert(relation->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
		   relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
		   relation->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

	RelationGetIndexList(relation);
	// Generate WHERE clause using new values of REPLICA IDENTITY
	if (OidIsValid(relation->rd_replidindex))
	{
		Relation    indexRel;
		int			key;

		// Use all the values associated with the index
		indexRel = index_open(relation->rd_replidindex, ShareLock);
		for (key = 0; key < indexRel->rd_index->indnatts; key++)
		{
			int	relattr = indexRel->rd_index->indkey.values[key - 1];

			// For a relation having REPLICA IDENTITY set at DEFAULT
			// or INDEX, if one of the columns used for tuple selectivity
			// is changed, the old tuple data is not NULL and need to
			// be used for tuple selectivity. If no such columns are
			// updated, old tuple data is NULL.
			print_where_clause_item(json, relation,
									oldtuple ? oldtuple : newtuple,
									relattr);
		}
		index_close(indexRel, NoLock);
		return;
	}

	// We need absolutely some values for tuple selectivity now
	Assert(oldtuple != NULL && relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL);

	// Fallback to default case, use of old values and print WHERE clause
	// using all the columns. This is actually the code path for FULL.
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		print_where_clause_item(json, relation, oldtuple, natt);
	}
}

/*
 * Decode an INSERT entry
 */
static void
ppl_insert(LogicalDecodingContext *ctx,
               Relation relation,
               HeapTuple tuple)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	json_object              *json = json_object_new_object();
	TupleDesc		          tupdesc = RelationGetDescr(relation);
	int                       natt;

	json_object *json_relation;
	json_object *val;
	const char *name;

	// Add insert object to context
	char *iter = data->numstrings[data->json_iter++];
	json_object_object_add(data->json, iter, json);

	// Query header
	json_relation= print_relname(relation);
	json_object_object_add(data->json, "@insert", json_relation);

	// Build column names and values
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		json_object       *json = json_object_new_object();
		Form_pg_attribute  attr = tupdesc->attrs[natt];
		Datum              origval;
		bool               isnull;

		// Skip dropped columns and system columns
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		// Get Datum from tuple
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);
		// Get output function
		val = print_value(origval, attr->atttypid, isnull);

		name = quote_identifier(NameStr(attr->attname));
		json_object_object_add(json, name, val);
	}
}

/*
 * Decode a DELETE entry
 */
static void
ppl_delete(LogicalDecodingContext *ctx,
               Relation relation,
               HeapTuple tuple)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	json_object              *json = json_object_new_object();
	json_object              *json_relation;

	// Add insert object to context
	char *iter = data->numstrings[data->json_iter++];
	json_object_object_add(data->json, iter, json);

	// Query header
	json_relation= print_relname(relation);
	json_object_object_add(data->json, "@delete", json_relation);

	// Here the same tuple is used as old and new values, selectivity will
	// be properly reduced by relation uses DEFAULT or INDEX as REPLICA
	// IDENTITY.
	print_where_clause(json, relation, tuple, tuple);
}


/*
 * Decode an UPDATE entry
 */
static void
ppl_update(LogicalDecodingContext *ctx,
               Relation relation,
               HeapTuple oldtuple,
               HeapTuple newtuple)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	TupleDesc                 tupdesc = RelationGetDescr(relation);
	int                       natt;
	json_object              *json, *json_val;
	const char                     *name;

	// If there are no new values, simply leave as there is nothing to do
	if (newtuple == NULL)
		return;

	// json object for update
	json = json_object_new_object();

	// add insert object to context
	name = data->numstrings[data->json_iter++];
	json_object_object_add(data->json, name, json);

	// relation header
	json_val= print_relname(relation);
	json_object_object_add(data->json, "@update", json_val);

	// Build the SET clause with the new values
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute	attr;
		Datum				origval;
		bool				isnull;

		attr = tupdesc->attrs[natt];

		// Skip dropped columns and system columns
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		name = quote_identifier(NameStr(attr->attname));

		// Get Datum from tuple
		origval = fastgetattr(newtuple, natt + 1, tupdesc, &isnull);

		// output
		json_val = print_value(origval, attr->atttypid, isnull);
		json_object_object_add(json, name, json_val);
	}

	// Print WHERE clause
	json_val = json_object_new_object();
	json_object_object_add(json, "@where", json_val);
	print_where_clause(json_val, relation, oldtuple, newtuple);
}
