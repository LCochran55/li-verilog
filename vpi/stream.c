//DATA STREAMING STUFF
//#include <glib.h>
#include <librdkafka/rdkafka.h>
#include "stream.h"

static rd_kafka_t *producer = NULL;
static rd_kafka_topic_t *rk_topic = NULL;
static const char *kafka_brokers = "localhost:9092";
static const char *kafka_topic_str = "vcd-topic";

static void dr_msg_cb (rd_kafka_t *kafka_handle,
                       const rd_kafka_message_t *rkmessage,
                       void *opaque) {
    if (rkmessage->err) {
        g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

void init_kafka(void) {
  //To be init at startup, before open_dump_file called
  //Create conf, set the bootstrap server, create producer, create topic; called after dump_file

  char errstr[512];

  rd_kafka_conf_t *rk_conf = rd_kafka_conf_new();
  if (!rk_conf) {
        g_error("Failed to create kafka conf");
        return;
  }

  if (rd_kafka_conf_set(rk_conf, "bootstrap.servers", kafka_brokers,
                      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    rd_kafka_conf_destroy(rk_conf);
    g_error("Failed to set config: %s", errstr);
    return;
  }

  if (rd_kafka_conf_set(rk_conf, "acks", "all",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(rk_conf);
        g_error("Failed to set acks: %s", errstr);
        return;
  }


  rd_kafka_conf_set_dr_msg_cb(rk_conf, dr_msg_cb);

  // Create the Producer instance.
  producer = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr));
    if (!producer) {
        rd_kafka_conf_destroy(rk_conf);
        g_error("Failed to create new producer: %s", errstr);
        return;
    }
    

  rk_topic = rd_kafka_topic_new(producer, kafka_topic_str, NULL);
  if (!rk_topic) {
        g_error("Failed to create topic handle: %s",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(producer);
        producer = NULL;
        return;
  }
}

void flush_kafka(void) {
  //Destorys kafka things at end of simulation
  //Flush and cleanup kakfa before calling comsumer close and kafka destroy
  if (!producer) return;

  rd_kafka_flush(producer, 10 * 1000);

  if (rk_topic) {
        rd_kafka_topic_destroy(rk_topic);
        rk_topic = NULL;
  }

  rd_kafka_destroy(producer);
  producer = NULL;
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

void kafka_stream_data(const char *vcd_text, size_t vcd_text_len) {
   int err = rd_kafka_produce(
        rk_topic,              
        RD_KAFKA_PARTITION_UA, 
        RD_KAFKA_MSG_F_COPY,   
        (void *)vcd_text, vcd_text_len, 
        NULL, 0,
        NULL
    );

  if (err == -1) {
       g_warning("Failed to produce to topic %s: %s", kafka_topic_str, rd_kafka_err2str(rd_kafka_last_error()));
  }

  rd_kafka_poll(producer, 0);
}
