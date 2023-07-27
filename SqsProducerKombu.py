import sys 
from kombu import Connection
import time 



global conn 
conn = None 

# creating connection object 
def connect(): 
    global conn
    conn = Connection("sqs://", transport_options={
                                    'region': sqs_region,
                                    'access_key': sqs_access_key,
                                    'secret_key': sqs_secret_key,
                                    })
def send_message_SQS(message):
    print("sending message to sqs")
   
    if not conn:
        connect()

    #creating producer_object 
    producer = conn.Producer()
    try:
        '''routing key is the queue name '''
        producer.publish(message,routing_key="CaesarQ")
  
    except Exception as e: 
        print(e)

if __name__ == "__main__":
    send_message_SQS(message="Hello World")
    
