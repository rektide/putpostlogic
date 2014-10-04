/*-------------------------------------------------------------------------
 *
 * putpostlogic.c
 *		Logical decoding output plugin generating JSON records of changes /w
 *		additional (Kafka and Nanomsg) output capabilities
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Copyright (c) 2012-2014, rektide de la faye
 *
 * IDENTIFICATION
 *		  putpostlogic.c
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
#include "nanomsg/pair.h"
#include "nanomsg/pubsub.h"
#include "nanomsg/pipeline.h"
#include "librdkafka/rdkafka.h"

#include "putpostlogic.h"
#include "decode.h"

#include "time.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void ppl_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void ppl_shutdown(LogicalDecodingContext *ctx);
static void ppl_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void ppl_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void ppl_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);

static double time_diff(struct timespec *current, struct timespec *old);
static char** startup_numstrings();
static void startup_kafka(PutPostLogicDecodingData *data, char *brokers, char *topic);
static int startup_nanomsg(char *url, int nn_protocol, bool bind);

static void txn_json_put_cb (rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque);

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

	cb->startup_cb = ppl_startup;
	cb->begin_cb = ppl_begin_txn;
	cb->change_cb = ppl_change;
	cb->commit_cb = ppl_commit_txn;
	cb->shutdown_cb = ppl_shutdown;
}

static char **numstrings = NULL;

static char **
startup_numstrings()
{
	if (numstrings == NULL)
	{
		char numstring[15];
		int  len;
		int  i = 0;

		numstrings = palloc(sizeof(char*) * 65536);
		if (numstrings[0] == 0)
		{
			for(i = 0; i < 2 << 15; ++i)
			{
				sprintf(numstring, "%d", i);
				len = strlen(numstring);
				numstrings[i] = palloc(len+1);
				strcpy(numstrings[i], numstring);
			}
		}
	}
	return numstrings;
}

static double
time_diff(struct timespec *current, struct timespec *old)
{
	return ((current->tv_sec - old->tv_sec) * 1.0e-9) + (current->tv_nsec - old->tv_nsec);
}

/* initialize this plugin */
static void
ppl_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	ListCell *option;
	PutPostLogicDecodingData *data;
	char *kafka_topic = NULL;
	char *kafka_brokers = NULL;
	char *nanomsg_pair = NULL;
	bool nanomsg_pair_bind = false;
	char *nanomsg_pub = NULL;
	char *nanomsg_pub_topic = NULL;
	bool nanomsg_pub_bind = false;
	char *nanomsg_push = NULL;
	bool nanomsg_push_bind = false;

	data = palloc(sizeof(PutPostLogicDecodingData));
	data->launch_time = palloc(sizeof(struct timespec));
	data->txn_time = palloc(sizeof(struct timespec));
	data->text_output = true;
	data->include_transaction = false;
	data->include_timestamp = false;
	data->json = NULL;
	data->json_iter = -1;
	data->json_topic = NULL;
	data->kafka = NULL;
	data->kafka_topic = NULL;
	data->nanomsg_pair = -1;
	data->nanomsg_pub = -1;
	data->nanomsg_push = -1;
	data->numstrings = startup_numstrings();
	ctx->output_plugin_private = data;
	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	clock_gettime(CLOCK_MONOTONIC, data->launch_time);

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
		else if (strcmp(elem->defname, "nanomsg-pair") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &nanomsg_pair;
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "nanomsg-pair-bind") == 0)
		{
			nanomsg_pair_bind = true;
		}
		else if (strcmp(elem->defname, "nanomsg-pub") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &nanomsg_pub;
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "nanomsg-pub-topic") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &nanomsg_pub_topic;
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "nanomsg-pub-bind") == 0)
		{
			nanomsg_pub_bind = true;
		}
		else if (strcmp(elem->defname, "nanomsg-push") == 0)
		{
			if (elem->arg != NULL)
			{
				dest = &nanomsg_push;
			}
			else
			{
				validArgs = false;
			}
		}
		else if (strcmp(elem->defname, "nanomsg-push-bind") == 0)
		{
			nanomsg_push_bind = true;
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
		else if (strcmp(elem->defname, "json-topic") == 0)
		{
			if (elem->arg != NULL)
			{
				char *val = strVal(elem->arg);
				data->json_topic = json_object_new_string(val);
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
			*dest = palloc(strlen(val));
			strcpy(*dest, val);
		}

	}

	if (kafka_topic != NULL)
	{
		startup_kafka(data, kafka_brokers, kafka_topic);
	}
	if (nanomsg_pair != NULL)
	{
		data->nanomsg_pair = startup_nanomsg(nanomsg_pair, NN_PAIR, nanomsg_pair_bind);
	}
	if (nanomsg_pub != NULL)
	{
		data->nanomsg_pub = startup_nanomsg(nanomsg_pub, NN_PUB, nanomsg_pub_bind);
	}
	if (nanomsg_push != NULL)
	{
		data->nanomsg_push = startup_nanomsg(nanomsg_push, NN_PUSH, nanomsg_push_bind);
	}
}

static int
startup_nanomsg(char *url, int nn_protocol, bool bind)
{
	int socket = nn_socket(AF_SP, nn_protocol);
	if (socket >= 0) {
		if (bind) {
			nn_bind(socket, url);
		}
		else
		{
			nn_connect(socket, url);
		}
	}
	return socket;
}

static void
startup_kafka(PutPostLogicDecodingData *data, char *brokers, char *topic)
{
	char errstr[512];
	char default_brokers[] = "localhost:9092";
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


	if (brokers == NULL)
	{
		brokers = default_brokers;
	}

	rd_kafka_conf_set_opaque(conf, data);
	rd_kafka_conf_set_dr_cb(conf, &txn_json_put_cb);

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
		if (ok && rd_kafka_topic_conf_set(topic_conf, topic_keys[i], topic_values[i], errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
			         errmsg("kafka topic configuration failed: \"%s\"", topic_keys[i])));
			ok = false;
		}
	}

	if (ok && (rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr))) == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
		         errmsg("kafka producer create failed: \"%s\"", (char*)&errstr)));
		ok = false;
	}

	if (ok && rd_kafka_brokers_add(rk, brokers) == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
		         errmsg("kafka broker add failed, \"%s\" resulted in: %s", brokers, (char*)&errstr)));
		ok = false;
	}
	if (ok && (rkt = rd_kafka_topic_new(rk, topic, topic_conf)) == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
		         errmsg("kafka topic add failed, \"%s\" resulted in: %s", brokers, (char*)&errstr)));
		ok = false;
	}

	if (ok)
	{
		data->kafka = rk;
		data->kafka_topic = rkt;
	}
	else
	{
		if (rkt)
		{
			rd_kafka_topic_destroy(rkt);
		}
		if (rk)
		{
			rd_kafka_destroy(rk);
		}
		rd_kafka_topic_conf_destroy(topic_conf);
		rd_kafka_conf_destroy(conf);
	}
}

/* cleanup this plugin's resources */
static void
ppl_shutdown(LogicalDecodingContext *ctx)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	int wait = 20000;

	nn_term();

	while (data->kafka && wait > 0 && rd_kafka_outq_len(data->kafka) > 0)
	{
		rd_kafka_poll(data->kafka, 50);
		wait -= 50;
	}
	if (data->kafka_topic)
	{
		rd_kafka_topic_destroy(data->kafka_topic);
	}
	if (data->kafka)
	{
		rd_kafka_destroy(data->kafka);
		rd_kafka_wait_destroyed(2000);
	}

	json_object_put(data->json_topic);
}

/* BEGIN callback */
static void
ppl_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;

	data->json = json_object_new_object();

	if (data->include_transaction)
	{
		json_object *xid = json_object_new_int(txn->xid);
		json_object_object_add(data->json, "@xid", xid);
	}

	if (data->include_timestamp)
	{
		clock_gettime(CLOCK_MONOTONIC, data->txn_time);
	}

	if (data->json_topic != NULL)
	{
		json_object_get(data->json_topic);
		json_object_object_add(data->json, "_topic", data->json_topic);
	}

	data->json_iter = 0;

	// Drain kafka events
	rd_kafka_poll(data->kafka, 0);
}

/* COMMIT callback */
static void
ppl_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	PutPostLogicDecodingData *data = ctx->output_plugin_private;
	const char               *json = json_object_to_json_string(data->json);
	int                       len = strlen(json);

	if (data->include_timestamp)
	{
		struct timespec end = {0,0};
		json_object *time_start, *time_duration;
		clock_gettime(CLOCK_MONOTONIC, &end);

		time_start = json_object_new_double(time_diff(data->txn_time, data->launch_time));
		json_object_object_add(data->json, "@ts", time_start);
		time_duration = json_object_new_double(time_diff(&end, data->txn_time));
		json_object_object_add(data->json, "@td", time_duration);
	}

	json = json_object_to_json_string(data->json);
	len = strlen(json);

	if (data->text_output) {
		OutputPluginPrepareWrite(ctx, true);
		appendStringInfoString(ctx->out, json);
		OutputPluginWrite(ctx, true);
	}

	if (data->nanomsg_pair >= 0) {
		nn_send(data->nanomsg_pair, json, len, 0);
	}
	if (data->nanomsg_pub >= 0) {
		nn_send(data->nanomsg_pub, json, len, 0);
	}

	if (data->nanomsg_push >= 0) {
		nn_send(data->nanomsg_push, json, len, 0);
	}

	if (data->kafka_topic != NULL)
	{
		rd_kafka_produce(data->kafka_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, (char *)json, len, NULL, 0, data->json);
	}
	else
	{
		json_object_put(data->json);
	}
	data->json = NULL;
}

/* Completion callback */
static void
txn_json_put_cb (rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque)
{
	json_object_put(msg_opaque);
}

/*
 * Callback for individual changed tuples
 */
static void
ppl_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation, ReorderBufferChange *change)
{
	//MemoryContext	old;
	char			replident = relation->rd_rel->relreplident;
	bool			is_rel_non_selective;

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

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (change->data.tp.newtuple != NULL)
			{
				ppl_insert(ctx, relation, &change->data.tp.newtuple->tuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!is_rel_non_selective)
			{
				HeapTuple oldtuple = change->data.tp.oldtuple != NULL ?
					&change->data.tp.oldtuple->tuple : NULL;
				HeapTuple newtuple = change->data.tp.newtuple != NULL ?
					&change->data.tp.newtuple->tuple : NULL;

				ppl_update(ctx, relation, oldtuple, newtuple);

			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!is_rel_non_selective)
			{
				ppl_delete(ctx, relation, &change->data.tp.oldtuple->tuple);
			}
			break;
		default:
			/* Should not come here */
			Assert(0);
			break;
	}
}
