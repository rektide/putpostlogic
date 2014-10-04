#ifndef PUTPOSTLOGIC_H_
#define PUTPOSTLOGIC_H_

#include "json-c/json.h"
#include "librdkafka/rdkafka.h"

/*
 * Structure storing the plugin specifications and options.
 */
typedef struct
{
	bool              text_output;
	bool              include_transaction;
	bool              include_timestamp;
	json_object      *json;
	int               json_iter; // cursor
	json_object      *json_topic; // topic property
	rd_kafka_t       *kafka;
	rd_kafka_topic_t *kafka_topic;
	int               nanomsg_pair; // nn_pair socket
	int               nanomsg_pub; // nn_pub socket
	int               nanomsg_push; // nn_push socket
	char            **numstrings;
} PutPostLogicDecodingData;

#endif
