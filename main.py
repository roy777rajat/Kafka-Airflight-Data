import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer




#check if the script is running in a virtual environment
if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    print("Running in a virtual environment")
else:
    print("Not running in a virtual environment")

#produce messages to kafka topic
def produce_messages(topic_name, num_messages=10):
    try:
        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
        producer = KafkaProducer()  
    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
        return
    try:
        producer.bootstrap_connected()
        print("Kafka broker is connected")
    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
        return
    ack = producer.send(topic_name, value="Hello World Tata Consultancy!!".encode())
    metadata =ack.get()
    print(metadata.topic)
    print(metadata.partition)   
    producer.flush()
    print("All messages produced successfully")
    


#main function to consume messages from kafka topic
def consume_messages(topic_name):
    print("Consuming messages from topic:", topic_name)
    try:    
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
        )
        consumer.subscribe([topic_name])
        print("Subscribed to topic:", topic_name)   
        consumer.poll(timeout_ms=1000)  # Poll for messages
        print("Polling for messages...",consumer)
        for message in consumer:
            print(f"Consumed: {message.value}")
        time.sleep(1)  # Sleep for 1 second to allow messages to be consumed

    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
         
    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
        return

    
    consumer.close()
    consumer.commit()  # Commit the offsets after consuming messages

#main function to run the script
def main():
    topic_name = os.getenv('KAFKA_TOPIC')
    print(f"Topic name: {topic_name}")
    print(f"Bootstrap servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")

    if not topic_name:
        print("KAFKA_TOPIC_NAME environment variable is not set")
        return

    # Produce messages to the topic
    produce_messages(topic_name)

    # Consume messages from the topic
    consume_messages(topic_name)    

if __name__ == "__main__":
    main()  
    