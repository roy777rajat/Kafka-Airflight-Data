from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
import time
import pandas as pd
import json


def read_csv_file(file_path,size):
    try:
        df = pd.read_csv(file_path, chunksize=size)
        return df
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Empty data in file: {file_path}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None 
    

def produce_messages(topic_name, airplane_chuk):
            try:
                producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092' , value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            except KafkaError as e:
                print(f"Error connecting to Kafka broker: {e}")
                producer = KafkaProducer()  
                return
            try:
                producer.bootstrap_connected()
                print("Kafka broker is connected")
            except KafkaError as e:
                print(f"Error connecting to Kafka broker: {e}")
                return
            
            ack = producer.send(topic_name, value=airplane_chuk)
            metadata = ack.get()
            #Get the message delivered report
            print(f"Message produced successfully to topic {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
            producer.flush()
            producer.close()
            return metadata.offset 
            
#Main function to run the script
def main():
    topic_name = os.getenv('KAFKA_TOPIC')
    file_path = os.getenv('CSV_FILE_PATH')
    file_name = os.path.basename(file_path)
    print(f"File name: {file_name} starting prcessing and producing messages to topic: {topic_name}")
    chunksize=10
    airplane_chunk_text = read_csv_file(file_path,chunksize)
    
    if airplane_chunk_text is not None:
       
       for airplane_chunk in airplane_chunk_text:
            print("#############Starting to produce messages#############")
            airplnae_dict = airplane_chunk.to_dict(orient='records')[0]
            offestcount =produce_messages(topic_name, airplnae_dict)
            time.sleep(2)
            
            if offestcount > 65:
                break;
    
    else:
        print("Failed to read CSV file.")


if __name__ == "__main__":
    main()
    

