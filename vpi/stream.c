//DATA STREAMING STUFF
#include <glib.h>
#include <librdkafka/rdkafka.h>


static rd_kafka_t *producer = NULL; 
static rd_kafka_topic_t *rk_topic = NULL; 
static rd_kafka_conf_t *rk_conf = NULL; 
static const char *kafka_brokers = "localhost:9092";
static const char *kafka_topic = "vcd-topic";

static void init_kafka() {
  //To be init at startup, before open_dump_file called

  //Create conf, set the bootstrap server, create producer, create topic; called after dump_file
  char errstr[512];
  rk_conf = rd_kafka_conf_new();
  if (!rk_conf) return -1;

  set_config(conf, "bootstrap.servers", kafka_brokers); 
  set_config(conf, "acks", "all");

  rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

  // Create the Producer instance.
  producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        rd_kafka_conf_destory(producer);
        g_error("Failed to create new producer: %s", errstr);
        return 1;
    }
  
  // Configuration object is now owned, and freed, by the rd_kafka_t instance.
  rk_conf = NULL;

  rk_topic = rd_kafka_topic_new(producer, kafka_topic, NULL);
}

static void flush_kafka() {
  //Destorys kafka things at end of simulation
  //Flush and cleanup kakfa before calling comsumer close and kafka destroy
  rd_kafka_flush(producer, 10*1000);
  if (rk_topic) { 
    rd_kafka_topic_destroy(rk_topic);
  }
  rd_kafka_destroy(producer);

} 

// Consumer termination:
// rd_kafka_consumer_close() and rd_kafka_destroy():


// Producer termination squence:
/* 
 1) Make sure all outstanding requests are transmitted and handled.
  rd_kafka_flush(rk, 60*1000); /* One minute timeout 
 
 2) Destroy the topic and handle objects 
  rd_kafka_topic_destroy(rkt); /* Repeat for all topic objects held 
  rd_kafka_destroy(rk);
*/

static void kafka_stream_data(void) {
  //Queue message to kafka
  //
}
