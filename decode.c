/*-------------------------------------------------------------------------
 *
 * decode.c
 *		Logical decoding output plugin generating SQL queries based
 *		on things decoded.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  decoder_raw/decoder_raw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if standard_conforming_strings were enabled.
 */
static void
print_literal(StringInfo s, Oid typid, char *outputstr)
{
	const char *valptr;

	switch (typid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			// NB: We don't care about Inf, NaN et al.
			appendStringInfoString(s, outputstr);
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(s, "B'%s'", outputstr);
			break;

		default:
			appendStringInfoChar(s, '\'');
			for (valptr = outputstr; *valptr; valptr++)
			{
				char		ch = *valptr;

				if (SQL_STR_DOUBLE(ch, false))
					appendStringInfoChar(s, ch);
				appendStringInfoChar(s, ch);
			}
			appendStringInfoChar(s, '\'');
			break;
	}
}

/*
 * Print a relation name into the StringInfo provided by caller.
 */
static void
print_relname(StringInfo s, Relation rel)
{
	Form_pg_class	class_form = RelationGetForm(rel);

	appendStringInfoString(s,
		quote_qualified_identifier(
				get_namespace_name(
						   get_rel_namespace(RelationGetRelid(rel))),
			NameStr(class_form->relname)));
}

/*
 * Print a value into the StringInfo provided by caller.
 */
static void
print_value(StringInfo s, Datum origval, Oid typid, bool isnull)
{
	Oid					typoutput;
	bool				typisvarlena;

	// Query output function
	getTypeOutputInfo(typid,
					  &typoutput, &typisvarlena);

	// Print value
	if (isnull)
		appendStringInfoString(s, "null");
	else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
		appendStringInfoString(s, "unchanged-toast-datum");
	else if (!typisvarlena)
		print_literal(s, typid,
					  OidOutputFunctionCall(typoutput, origval));
	else
	{
		// Definitely detoasted Datum
		Datum		val;
		val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
	}
}

/*
 * Print a WHERE clause item
 */
static void
print_where_clause_item(StringInfo s,
						Relation relation,
						HeapTuple tuple,
						int natt,
						bool *first_column)
{
	Form_pg_attribute	attr;
	Datum				origval;
	bool				isnull;
	TupleDesc			tupdesc = RelationGetDescr(relation);

	attr = tupdesc->attrs[natt];

	// Skip dropped columns and system columns
	if (attr->attisdropped || attr->attnum < 0)
		return;

	// Skip comma for first colums
	if (!*first_column)
		appendStringInfoString(s, " AND ");
	else
		*first_column = false;

	// Print attribute name
	appendStringInfo(s, "%s = ", quote_identifier(NameStr(attr->attname)));

	// Get Datum from tuple
	origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

	// Get output function
	print_value(s, origval, attr->atttypid, isnull);
}

/*
 * Generate a WHERE clause for UPDATE or DELETE.
 */
static void
print_where_clause(StringInfo s,
				   Relation relation,
				   HeapTuple oldtuple,
				   HeapTuple newtuple)
{
	TupleDesc		tupdesc = RelationGetDescr(relation);
	int				natt;
	bool			first_column = true;

	Assert(relation->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT ||
		   relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL ||
		   relation->rd_rel->relreplident == REPLICA_IDENTITY_INDEX);

	// Build the WHERE clause
	appendStringInfoString(s, " WHERE ");

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
			//
			print_where_clause_item(s, relation,
									oldtuple ? oldtuple : newtuple,
									relattr, &first_column);
		}
		index_close(indexRel, NoLock);
		return;
	}

	// We need absolutely some values for tuple selectivity now
	Assert(oldtuple != NULL && relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL);

	// Fallback to default case, use of old values and print WHERE clause
	// using all the columns. This is actually the code path for FULL.
	//
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		print_where_clause_item(s, relation, oldtuple, natt, &first_column);
	}
}

/*
 * Decode an INSERT entry
 */
static void
ppl_insert(StringInfo s,
				   Relation relation,
				   HeapTuple tuple)
{
	TupleDesc		tupdesc = RelationGetDescr(relation);
	int				natt;
	bool			first_column = true;
	StringInfo		values = makeStringInfo();

	// Initialize string info for values
	initStringInfo(values);

	// Query heade
	appendStringInfo(s, "INSERT INTO ");
	print_relname(s, relation);
	appendStringInfo(s, " (");

	// Build column names and values
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute	attr;
		Datum				origval;
		bool				isnull;

		attr = tupdesc->attrs[natt];

		// Skip dropped columns and system columns
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		// Skip comma for first colums
		if (!first_column)
		{
			appendStringInfoString(s, ", ");
			appendStringInfoString(values, ", ");
		}
		else
			first_column = false;

		// Print attribute name
		appendStringInfo(s, "%s", quote_identifier(NameStr(attr->attname)));

		// Get Datum from tuple
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		// Get output function
		print_value(values, origval, attr->atttypid, isnull);
	}

	// Append values
	appendStringInfo(s, ") VALUES (%s);", values->data);

	// Clean up
	resetStringInfo(values);
}

/*
 * Decode a DELETE entry
 */
static void
ppl_delete(StringInfo s,
				   Relation relation,
				   HeapTuple tuple)
{
	appendStringInfo(s, "DELETE FROM ");
	print_relname(s, relation);

	// Here the same tuple is used as old and new values, selectivity will
	// be properly reduced by relation uses DEFAULT or INDEX as REPLICA
	// IDENTITY.
	//
	print_where_clause(s, relation, tuple, tuple);
	appendStringInfoString(s, ";");
}


/*
 * Decode an UPDATE entry
 */
static void
ppl_update(StringInfo s,
				   Relation relation,
				   HeapTuple oldtuple,
				   HeapTuple newtuple)
{
	TupleDesc		tupdesc = RelationGetDescr(relation);
	int				natt;
	bool			first_column = true;

	// If there are no new values, simply leave as there is nothing to do
	if (newtuple == NULL)
		return;

	appendStringInfo(s, "UPDATE ");
	print_relname(s, relation);

	// Build the SET clause with the new values
	appendStringInfo(s, " SET ");
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute	attr;
		Datum				origval;
		bool				isnull;

		attr = tupdesc->attrs[natt];

		// Skip dropped columns and system columns
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		// Skip comma for first colums
		if (!first_column)
		{
			appendStringInfoString(s, ", ");
		}
		else
			first_column = false;

		// Print attribute name
		appendStringInfo(s, "%s = ", quote_identifier(NameStr(attr->attname)));

		// Get Datum from tuple
		origval = fastgetattr(newtuple, natt + 1, tupdesc, &isnull);

		// Get output function
		print_value(s, origval, attr->atttypid, isnull);
	}

	// Print WHERE clause
	print_where_clause(s, relation, oldtuple, newtuple);

	appendStringInfoString(s, ";");
}
