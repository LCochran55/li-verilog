#ifndef STREAM_H
#define STREAM_H

#include <librdkafka/rdkafka.h>
#include <stddef.h>


void init_kafka(void);
void flush_kafka(void);

void kafka_stream_data(const char *payload, size_t len);

#endif
