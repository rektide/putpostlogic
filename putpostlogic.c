/*-------------------------------------------------------------------------
 *
 * ppl_raw.c
 *		Logical decoding output plugin generating SQL queries based
 *		on things decoded.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  ppl_raw/ppl_raw.c
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

#include "json-c/json.h"
#include "nanomsg/nn.h"
#include "nanomsg/pipeline.h"
#include "librdkafka/rdkafka.h"

#include "putpostlogic.h"
#include "decode.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/*
 * Structure storing the plugin specifications and options.
 */
typedef struct
{
	bool              text_output;
	bool              include_transaction;
	bool              include_timestamp;
	json_object      *json;
	rd_kafka_t       *kafka;
	rd_kafka_topic_t *kafka_topic;
	int               nanomsg_socket;
} PutPostLogicDecodingData;

static void ppl_raw_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void ppl_raw_shutdown(LogicalDecodingContext *ctx);
static void ppl_raw_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void ppl_raw_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void ppl_raw_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);

static void startup_kafka(PutPostLogicDecodingData *data, char *brokers, char *topic);
static void startup_nanomsg(PutPostLogicDecodingData *data, char *url);

void
_PG_init(void)
{
	/* other plugins can perform things here */
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = ppl_raw_startup;
	cb->begin_cb = ppl_raw_begin_txn;
	cb->change_cb = ppl_raw_change;
	cb->commit_cb = ppl_raw_commit_txn;
	cb->shutdown_cb = ppl_raw_shutdown;
}


/* initialize this plugin */
static void
ppl_raw_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	ListCell *option;
	PutPostLogicDecodingData *data;
	char *kafka_topic;
	char *kafka_brokers;
	char *nanomsg_url;

	data = palloc(sizeof(PutPostLogicDecodingData));
	data->text_output = true;
	data->include_transaction = false;
	data->include_timestamp = false;
	data->kafka = NULL;
	data->kafka_topic = NULL;
	data->nanomsg_socket = -1;
	ctx->output_plugin_private = data;
	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem* elem = lfirst(option);
		bool     validArgs = true;
		char**   dest = NULL;
		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-transaction") == 0)
		{
			if (elem->arg != NULL)
			{
				if(!parse_bool(strVal(elem->arg), &data->include_transaction))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname)));
				}
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "nanomsg-url") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &nanomsg_url;
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "kafka-topic") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &kafka_topic;
			}
			else
			{
				validArgs = false;

			}
		}
		else if (strcmp(elem->defname, "kafka-brokers") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &kafka_brokers;
			}
			else
			{
				validArgs = false;
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}

		if (!validArgs)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" lacked needed value", elem->defname)));
		}
		else if (dest != NULL)
		{
			char* val = strVal(elem->arg);
			*dest = malloc(strlen(val));
			strcpy(*dest, val);

		}

	}

	if (kafka_topic != NULL)
	{
		startup_kafka(data, kafka_brokers, kafka_topic);
	}

	if (nanomsg_url != NULL)
	{
		startup_nanomsg(data, nanomsg_url);
	}
}

static void
startup_kafka(PutPostLogicDecodingData *data, char *brokers, char *topic)
{
	char errstr[512];
	char default_brokers = 'localhost:9092';
	rd_kafka_conf_t *conf = rd_kafka_conf_new();
	rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;

	char global_keys[][32] = {"compression.codec", "queue.buffering.max.ms", "message.send.max.retries"};
	char global_values[][32] = {"snappy", "166", "6"};
	int global_len = sizeof(global_keys) / sizeof(global_keys[0]);

	char topic_keys[][32] = {"message.timeout.ms"};
	char topic_values[][32] = {"1800000"};
	int topic_len = sizeof(topic_keys) / sizeof(topic_keys[0]);
	int i;

	bool ok = true;

	for (i = 0; i < global_len; ++i)
	{
		if (rd_kafka_conf_set(conf, global_keys[i], global_values[i], errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
			         errmsg("kafka configuration failed: \"%s\"", global_keys[i])));
			ok = false;
		}
	}
	for (i = 0; i < topic_len; ++i)
	{
		if (rd_kafka_topic_conf_set(conf, topic_keys[i], topic_values[i], errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
			         errmsg("kafka topic configuration failed: \"%s\"", topic_keys[i])));
			ok = false;
		}
	}

	if (!ok || !(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr))))
	{
		if (ok) ereport(ERROR,
		                 (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
		                  errmsg("kafka producer create failed: \"%s\"", (char*)&errstr)));
		ok = false;
	}

	if (brokers == NULL)
	{
		brokers = &default_brokers;
	}
	if (!ok || rd_kafka_brokers_add(rk, brokers) == 0) {
		if (ok) ereport(ERROR,
		                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
		                 errmsg("kafka broker add failed, \"%s\" resulted in:", brokers, (char*)&errstr)));
		ok = false;
	}

	/* Create topic */
	if (ok) {
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);
		if(rkt)
		{
			data->kafka = rk;
			data->kafka_topic = rkt;
		}
		else
		{
			rd_kafka_destroy(rk);
			ok = false;
		}
	}
}

static void
startup_nanomsg(PutPostLogicDecodingData *data, char *url)
{
	bool ok = true;
	int socket = nn_socket(AF_SP, NN_PUSH);
	if (socket == 0)
	{
		ok = false;
	}
	else if (nn_connect (socket, url) >= 0)
	{
		data->nanomsg_socket = socket;
	}
}

/* cleanup this plugin's resources */
static void
ppl_raw_shutdown(LogicalDecodingContext *ctx)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	int wait = 5000;

	if (data->nanomsg_socket)
	{
		nn_shutdown(data->nanomsg_socket, 0);
	}

	while (data->kafka && wait > 0 && rd_kafka_outq_len(data->kafka) > 0)
	{
		rd_kafka_poll(data->kafka, 50);
		wait -= 50;
	}
	if (data->kafka_topic)
	{
		rd_kafka_topic_conf_destroy(data->kafka_topic);
	}
	if (data->kafka)
	{
		rd_kafka_destroy(data->kafka);
		rd_kafka_wait_destroyed(2000);
	}
}

/* BEGIN callback */
static void
ppl_raw_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	data->json = json_object_new_object();

	if (data->include_transaction)
	{
		json_object* xid = json_object_new_int(txn->xid);
		json_object_object_add(data->json, "_xid", xid);
	}
}

/* COMMIT callback */
static void
ppl_raw_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	char *json = json_object_to_json_string(data->json);
	int len = strlen(json);
	json_object_put(data->json);

	if (data->text_output) {
		OutputPluginPrepareWrite(ctx, true);
		appendStringInfoString(ctx->out, json);
		OutputPluginWrite(ctx, true);
	}

	if (data->nanomsg_socket > 0)
	{
		int bytes = nn_send(data->nanomsg_socket, json, len, 0);
	}

	if (data->kafka_topic != NULL)
	{
		int status = rd_kafka_produce(data->kafka_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, json, len, NULL, 0, NULL);
	} else {
		free(json);
	}
}

/*
 * Callback for individual changed tuples
 */
static void
ppl_raw_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, ReorderBufferChange *change)
{
	PutPostLogicDecodingData* data;
	MemoryContext	old;
	char			replident = relation->rd_rel->relreplident;
	bool			is_rel_non_selective;

	data = ctx->output_plugin_private;

	/*
	 * Determine if relation is selective enough for WHERE clause generation
	 * in UPDATE and DELETE cases. A non-selective relation uses REPLICA
	 * IDENTITY set as NOTHING, or DEFAULT without an available replica
	 * identity index.
	 */
	RelationGetIndexList(relation);
	is_rel_non_selective = (replident == REPLICA_IDENTITY_NOTHING ||
							(replident == REPLICA_IDENTITY_DEFAULT &&
							 !OidIsValid(relation->rd_replidindex)));

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (change->data.tp.newtuple != NULL)
			{
				OutputPluginPrepareWrite(ctx, true);
				ppl_raw_insert(ctx->out,
								   relation,
								   &change->data.tp.newtuple->tuple);
				OutputPluginWrite(ctx, true);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!is_rel_non_selective)
			{
				HeapTuple oldtuple = change->data.tp.oldtuple != NULL ?
					&change->data.tp.oldtuple->tuple : NULL;
				HeapTuple newtuple = change->data.tp.newtuple != NULL ?
					&change->data.tp.newtuple->tuple : NULL;

				OutputPluginPrepareWrite(ctx, true);
				ppl_raw_update(ctx->out,
								   relation,
								   oldtuple,
								   newtuple);
				OutputPluginWrite(ctx, true);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!is_rel_non_selective)
			{
				OutputPluginPrepareWrite(ctx, true);
				ppl_raw_delete(ctx->out,
								   relation,
								   &change->data.tp.oldtuple->tuple);
				OutputPluginWrite(ctx, true);
			}
			break;
		default:
			/* Should not come here */
			Assert(0);
			break;
	}
}
