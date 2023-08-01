from kombu import Connection, Exchange, Producer, Queue
import time
import os 
import json 
from collections import defaultdict


sqs_region = 'us-east-2'
sqs_access_key = 'AKIA5AU75AEOLPJVFVGD'
sqs_secret_key = 'fYktNgrZTSFGLZwEjh44sV3+E6GkfpORz'

global conn 
conn =None 
consumer = None

''' both connection and consumer objects are cached'''

'''connect object can take protocol and transport option , we can create a separate file having the protocol and  transport option for each messaging service and fetch those based on the message broker neede'''

# creating connection object 
def connect(): 
    global conn
    conn = Connection("sqs://", transport_options={
                                    'region': sqs_region,
                                    'access_key': sqs_access_key,
                                    'secret_key': sqs_secret_key,
                                    })

def lambda_handler():
    global consumer
    if not conn:
        connect()
        
    queue_name = 'CaesarQ'
    queue = Queue(queue_name)

    def read_from_word_freq_file():
        if os.path.exists('/tmp/word_freq'):
            with open('/tmp/word_freq', "r") as file:
                return json.load(file)
        else:
            return defaultdict(int)


    def write_word_freq_to_file(word_freq):
        try:
            with open('/tmp/word_freq', 'w') as file:
                json.dump(word_freq, file)
            print("Content successfully written to the file.")

        except FileNotFoundError:
            with open('/tmp/word_freq', 'w') as file:
                json.dump(word_freq, file)
            print("File created and content written successfully.")

    def update_word_frequency(message, word_freq):
        print(message)
        for word in message.split():
            print(word)
            word_freq[word] = 1 + word_freq.get(word, 0)

    def process_message(body, message): 
        try:
            print(f"Received message: {body}")
            word_freq = read_from_word_freq_file()
            print(word_freq)
            update_word_frequency(body.lower(), word_freq)
            write_word_freq_to_file(word_freq)
            message.ack()

        except Exception as e:
            # Handle any exceptions that occur during processing
            print(f"Error processing message: {str(e)}")
            message.requeue()

    # Create a consumer
    if not consumer:
        consumer = conn.Consumer(queues=queue, callbacks=[process_message])

    # Start the consumer
    with consumer:
        # Wait for the consumer to process the messages , this receives multiple messages frosm the sqs and send the message to the callback functions to process and acknowledge that 
        # call receive_message with max_message_count 
        conn.drain_events(timeout=20)

if __name__ == "__main__":
    lambda_handler()



