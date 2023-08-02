import codecs
from kombu import Connection
import json
import time


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
            '''routing key'''
            ''' rabbitmq : routing_key'''
            '''sqs : queue'''
            '''kafka topic'''
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
            self.conn = Connection("sqs://", transport_options={
                                    'region': sqs_region,
                                    'access_key': sqs_access_key,
                                    'secret_key': sqs_secret_key,
                                    })
        queue_name = "withoutKombu"
        self.publish(msg,queue_name)

    def publish_to_rabbitMQ(self,msg):
        if not self.conn:
            self.conn = Connection("amqp://localhost:5672") 
        routing_key = "hello"
        self.publish(msg,routing_key)


    def publish_to_kafka(self,msg):
        if not self.conn:
            self.conn = Connection('confluentkafka://localhost:9092')
        topic = "hello"
        self.publish(msg,topic)

    def read_and_publish(self):
        publish_func = self.find_publish_function()
        combined_message_no = 0 
        combined_message = ""

        try:
            #using codecs to handle to non-unicode symbols in the file
            row_read_start_time = time.time()
            with codecs.open(r"C:\Users\sunit\Desktop\CapstoneProject\data\nearby-all-public-posts\salesData.csv",'r',encoding ='utf-8') as csv_file:
                for line in csv_file:
                    combined_message_no += 1 
                    combined_message += line

                    if combined_message_no >= self.no_of_message_to_combine:
                        #send_message to the messaging service 
                        publish_func(combined_message)
                        # SqsProducerKombu.send_message_SQS(combined_message)
                        combined_message = ""
                        combined_message_no = 0 

                time_elapsed = time.time() - row_read_start_time
                return time_elapsed
          
        except Exception as e: 
            print(e)

prod = producer(1000,"kafka")

for i in range(10):
    print(prod.read_and_publish())