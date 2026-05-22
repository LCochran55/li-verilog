//DATA STREAMING STUFF
#include <glib.h>
#include <librdkafka/rdkafka.h>

static rd_kafka_t *producer = NULL;
static rd_kafka_conf_t *conf = NULL;
static rd_kafka_topic_t *topic = NULL; 

static void init_kafka() {
  char errstr[512]
  conf = rd_kafka_conf_new();
  set_config(conf, "bootstrap.servers", "localhost:9092"); 
  set_config(conf, "acks", "all");

  rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

  // Create the Producer instance.
  producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        g_error("Failed to create new producer: %s", errstr);
        return 1;
    }
  
  // Configuration object is now owned, and freed, by the rd_kafka_t instance.
  conf = NULL;
}

static void destroy_kafka(void) {

}

static void kafka_send_data(void) {

  
}
