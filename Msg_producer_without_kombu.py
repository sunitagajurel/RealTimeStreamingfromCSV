import codecs
import boto3 
import pika 
from confluent_kafka import Producer
import time
from collections import defaultdict


sqs_region = 'us-east-2'
sqs_access_key = 'AKIA5AU75AEOLPJVFVGD'
sqs_secret_key = 'fYktNgrZTSFGLZwEjh44sV3+E6GkfpORz'

class producer():
    def __init__ (self,no_of_message_to_combine,messaging_service): 
        self.no_of_message_to_combine = no_of_message_to_combine
        self.conn = None
        self.messaging_service = messaging_service.lower()
   

    def publish(self,message,queue_name):
        producer = self.conn.Producer()
        try:
            '''routing key is the queue name '''
            producer.publish(message,routing_key=queue_name)

        except Exception as e: 
            print(e)

            
    ''' Finding publish function based on the input measaging service '''
    def find_publish_function(self):
        service = self.messaging_service
        if service == "sqs":
            return self.publish_to_sqs
        
        elif service == "rabbitmq":
            return  self.publish_to_rabbitMQ
        
        elif service == "kafka":
            return self.publish_to_kafka
            

    def publish_to_sqs(self,msg):
        if not self.conn: 
            self.conn = boto3.client('sqs',region_name="us-east-2")
       
        queue_url='https://sqs.us-east-2.amazonaws.com/894762287388/withoutKombu'

        self.conn.send_message(QueueUrl=queue_url,MessageBody= msg)


    def publish_to_rabbitMQ(self,msg):
        if not self.conn:
            conn =  pika.BlockingConnection(
                        pika.ConnectionParameters(host='localhost')) 
            channel = conn.channel()
        try:
            channel.basic_publish(exchange='', routing_key='hello',body=msg)
        except Exception as e:
            print(e)

   
    def publish_to_kafka(self,msg):
        if not self.conn:
            conf = {'bootstrap.servers':'localhost:9092'} 
            conn= Producer(**conf)
        conn.produce("hello",msg)
        conn.flush()

    def read_and_publish(self):
        publish_func = self.find_publish_function()
        combined_message_no = 0 
        combined_message = ""

        try:
            #using codecs to handle to non-unicode symbols in the file
            row_read_start_time = time.time()
            with codecs.open(r"C:\Users\sunit\Desktop\CapstoneProject\data\nearby-all-public-posts\posts_1.csv",'r',encoding ='utf-8') as csv_file:
                for line in csv_file:
                    pass
                time_elapsed = time.time() - row_read_start_time
                return time_elapsed
                    
                    # combined_message_no += 1 
                    # combined_message += line

                    # if combined_message_no >= self.no_of_message_to_combine:
                    #     #send_message to the messaging service 
                    #     publish_func(combined_message)
                    #     # SqsProducerKombu.send_message_SQS(combined_message)
                    #     print(f'published message:{combined_message}')
                    #     combined_message = ""
                    #     combined_message_no = 0 
          
        except Exception as e: 
            print(e)

prod = producer(1,"RabbitMQ")

for k in range(1,11):
    print(prod.read_and_publish())