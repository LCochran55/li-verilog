from confluent_kafka import Producer, Consumer, KafkaError
import subprocess


def stream_data():
    # Configuration for the consumer
    consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
    }    
    topic = 'vcd-topic'
    #Create consumer instance
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])



    with open("test_kafka_stream.v", "r") as v_files:
        v_mods = v_files.read()


    #Compile the Verilog file
    subprocess.run(["iverilog", "test_kafka_stream.v", "-o", "test_kafka_stream"])    
    #Run the compiled file
    subprocess.run(["vvp", "test_kafka_stream"])

    kafka_message_stream = []
    date_passed = False

    while True:
        message = consumer.poll(timeout=10.0)
        if message is None:
            break
        elif message.error():
            #Report that the test fails here too
            break
        if b"$version" in message.value() and date_passed == False:
            date_passed = True
        if date_passed: 
            #print(f"Received message: {message.value} from topic: {message.topic}")
            kafka_message_stream.extend(message.value().decode('utf-8').split('\n'))

    with open("test_kafka_stream.gold", "r") as gold:
        gold_lines = [line.strip() for line in gold]

    if gold_lines == kafka_message_stream:
        #TEST PASSED




