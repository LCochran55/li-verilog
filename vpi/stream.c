//DATA STREAMING STUFF
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include "stream.h"



/*
CURRENTLY TO START THE STREAM:
# In 1st terminal start consumer:
sudo docker exec kafka kafka-console-consumer \
    --topic vcd-topic \
    --bootstrap-server localhost:9092 \
    --from-beginning

# In 2nd terminal start simulation:
vvp simulation
*/

static rd_kafka_t *producer = NULL; 
static rd_kafka_topic_t *rk_topic = NULL;
static const char *kafka_brokers = NULL;
static const char *kafka_topic_str = "vcd-topic"; // Assuming the topic will stay the same for the broker idk kafka lol
                                                  // Just make the topic this when initlizing the broker

// delivery report callback
// triggered when a message had successfully delivered or if delivery had failed
static void dr_msg_cb (rd_kafka_t *kafka_handle,
                       const rd_kafka_message_t *rkmessage,
                       void *opaque) {
    if (rkmessage->err) {
        //fprintf(stderr, "Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
        return;
    }
}

void init_kafka(void) {
  //To be init at startup; create conf, set the bootstrap server, create producer, create topic
  kafka_brokers = getenv("KAFKA_BROKER");
  if (!kafka_brokers) kafka_brokers = "localhost:9092";

  char errstr[512]; //used to store error messages -> librdkafka API error reporting buffer

  rd_kafka_conf_t *rk_conf = rd_kafka_conf_new(); //creates a new configuration object for Kafka client
  if (!rk_conf) {
        //fprintf(stderr, "Failed to create kafka conf\n");
        return;  
  }

  //Set bootstrap broker(s) as a comma-separated list of host:port (port 9092).
  if (rd_kafka_conf_set(rk_conf, "bootstrap.servers", kafka_brokers,
                      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    rd_kafka_conf_destroy(rk_conf);
    //fprintf(stderr, "Failed to set config: %s", errstr);
    return;
  }

  /*Set the consumer group id.
         * All consumers sharing the same group id (acks) will join the same
         * group, and the subscribed topic' partitions will be assigned
         * according to the partition.assignment.strategy
         * (consumer config property) to the consumers in the group. */
  if (rd_kafka_conf_set(rk_conf, "acks", "all",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(rk_conf);
        //fprintf(stderr, "Failed to set acks: %s", errstr);
        return;
  }

   /* Set the delivery report callback.
         * This callback will be called once per message to inform
         * the application if delivery succeeded or failed.
         * See dr_msg_cb() above.
         * The callback is only triggered from rd_kafka_poll() and
         * rd_kafka_flush(). */
  rd_kafka_conf_set_dr_msg_cb(rk_conf, dr_msg_cb);

  // Create the Producer instance.
  producer = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr));
    if (!producer) {
        rd_kafka_conf_destroy(rk_conf);
        //fprintf(stderr, "Failed to create new producer: %s", errstr);
        return;
    }
    

  rk_topic = rd_kafka_topic_new(producer, kafka_topic_str, NULL);
  if (!rk_topic) {
        //fprintf(stderr, "Failed to create topic handle: %s",
                //rd_kafka_err2str(rd_kafka_last_error()));
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


// Producer termination squence:
/* 
 1) Make sure all outstanding requests are transmitted and handled.
  rd_kafka_flush(rk, 60*1000); /* One minute timeout 
 
 2) Destroy the topic and handle objects 
  rd_kafka_topic_destroy(rkt); /* Repeat for all topic objects held 
  rd_kafka_destroy(rk);
*/

void kafka_stream_data(const char *vcd_text, size_t vcd_text_len) {
  if (!producer) return;

   int err = rd_kafka_produce(
        rk_topic,              
        RD_KAFKA_PARTITION_UA, 
        RD_KAFKA_MSG_F_COPY,   
        (void *)vcd_text, vcd_text_len, 
        NULL, 0,
        NULL
    );

  // if (err == -1) {
       //fprintf(stderr, "Failed to produce to topic %s: %s", kafka_topic_str, rd_kafka_err2str(rd_kafka_last_error()));
      
  //}

  rd_kafka_poll(producer, 0);
}
