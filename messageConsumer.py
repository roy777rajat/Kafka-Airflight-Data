from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import time
import os
import boto3
import json
import sys
import pandas as pd
from dotenv import load_dotenv


load_dotenv()
TOPIC_NAME = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
FILE_NAME = os.getenv('S3_FILE_NAME')  
                              

def load_data_into_S3(data,filesuffix):
    s3 = boto3.client('s3')
    file_name = FILE_NAME
    file_name = file_name + "_"+ str(filesuffix) + ".json"
    print(f"File name: {file_name} starting prcessing and producing messages to topic: {TOPIC_NAME}")
    try:        
        if type(data) == str:
            try:    
                data = json.loads(data)
            except json.JSONDecodeError:
                print(f"Error decoding JSON: {data}")
                data = str(data)
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(data).encode('utf-8'))
        if type(data) == dict:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(data).encode('utf-8'))
        if type(data) == list:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(data).encode('utf-8'))
        if type(data) == bytes:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=data.decode('utf-8').encode('utf-8'))
        if type(data) == int:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == float:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == bool:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == None:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == object:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == complex:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == set:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        if type(data) == frozenset:
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=str(data).encode('utf-8'))
        print(f"Data loaded into S3 bucket {BUCKET_NAME} with file name {FILE_NAME}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Error uploading data to S3: {e}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Error uploading data to S3: {e}")
        
    except Exception as e:
        print(f"Error loading data into S3: {e}")

#main function to consume messages from kafka topic
def consume_messages(topic_name):
    try:    
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
            
        )
        consumer.subscribe([topic_name])
        
        consumer.poll(timeout_ms=1000)  # Poll for messages
        print("Polling for messages...",consumer)
        for i,count in enumerate(consumer):
            print(f"Consumed: {i} {count.value}")
            load_data_into_S3(count.value,i)
            time.sleep(2)
         
    except KafkaError as e:
        print(f"Error connecting to Kafka broker: {e}")
        return
    
    consumer.close()
    consumer.commit()  # Commit the offsets after consuming messages




#Main method to run the script
def main():
 
    if not TOPIC_NAME:
        print("Topic name is not set. Exiting...")
        return

    consume_messages(TOPIC_NAME)
    print("All messages consumed successfully")


#Call the main function to run the script
if __name__ == "__main__":
    main()