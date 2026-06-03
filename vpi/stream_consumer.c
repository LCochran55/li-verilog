//DATA STREAMING STUFF
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

static rd_kafka_t *consumer = NULL; 
static rd_kafka_topic_t *rk_topic = NULL;
static const char *kafka_brokers = NULL;
static const char *kafka_topic_str = "vcd-topic"; // Assuming the topic will stay the same for the broker idk kafka lol
                                                  // Just make the topic this when initlizing the broker

void init_consumer(void) {
  kafka_brokers = getenv("KAFKA_BROKER");
  if (!kafka_brokers) kafka_brokers = "localhost:9092";

  char errstr[512]; //used to store error messages -> librdkafka API error reporting buffer

  rd_kafka_conf_t *rk_conf = rd_kafka_conf_new(); //creates a new configuration object for Kafka client
  if (!rk_conf) {
        return;  
  } 

  if (rd_kafka_conf_set(rk_conf, "bootstrap.servers", kafka_brokers,
                      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    rd_kafka_conf_destroy(rk_conf);
    return;
  }

  if (rd_kafka_conf_set(rk_conf, "group.id", "vcd_stream",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      rd_kafka_conf_destroy(rk_conf);
      return;
  }

  if (rd_kafka_conf_set(rk_conf, "auto.offset.reset", "earliest",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      rd_kafka_conf_destroy(rk_conf);
      return;
  }

  consumer = rd_kafka_new(RD_KAFKA_CONSUMER, rk_conf, errstr, sizeof(errstr));
    if (!consumer) {
        rd_kafka_conf_destroy(rk_conf);
        return;
    }
 }

void flush_consumer(void) {
  if (!consumer) return;

  rd_kafka_flush(consumer, 10 * 1000);

  if (rk_topic) {
        rd_kafka_topic_destroy(rk_topic);
        rk_topic = NULL;
  }

  rd_kafka_destroy(consumer);
  consumer = NULL;
} 



void poll_consumer(void) {
  rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(consumer, 1000);
  if (!rkmessage){
    continue; // timeout: no message
  }

  printf("%s", rkmessage);
  fflush(stdout);

  rd_kafka_message_destroy(rkmessage);

  if ((++msg_count % 1000) == 0) {
    rd_kafka_resp_err_t err = rd_kafka_commit(consumer, NULL, 0);
    if (err) {
      // application-specific rollback of processed records
    }
  }
}

