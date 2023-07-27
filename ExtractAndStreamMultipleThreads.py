import threading 
import time 
import SqsProducerKombu
import codecs

#shared variables between producer and consumer 
capacity = 5
buffer = [-1 for i in range(capacity)]
in_index = 0 
out_index = 0 

# Declaring Semaphores

#allowing only one semaphore to operate on the shared buffer at a time.
mutex = threading.Semaphore()

#for producer to keep track of capacity , this will block the producer thread if the capacity is full 
empty_cells = threading.Semaphore(capacity)

#for consumer to keep track of capacity, this 
filled_cells = threading.Semaphore(0)


# Producer Thread Class
class Producer(threading.Thread):
  def run(self):
    
    global capacity, buffer, in_index, out_index
    global mutex, empty_cells, filled_cells
     
    try:
      with codecs.open(r"{file path}",'r',encoding ='utf-8') as csv_file:
        for line in csv_file:
            print(line)
            empty_cells.acquire()
            mutex.acquire()
            
            buffer[in_index] = line
            in_index = (in_index + 1)%capacity

            print("Producer produced : ", line)

            mutex.release()
            filled_cells.release()
            
            time.sleep(1)
    except KeyboardInterrupt as e:
      print(e)

class Consumer(threading.Thread):

  def __init__(self, combined_message_count):
     super(Consumer, self).__init__()
     self. combined_message_count =  combined_message_count

  def run(self):
    print("consumer started")
    global capacity, buffer, in_index, out_index, counter
    global mutex, empty_cells, filled_cells,producer_finished
    combined_message = ""
    msg_no = 0 
    msg_consumed = 0


    #when to stop consuming the message 
 
    while msg_consumed < 16:
      filled_cells.acquire()
      mutex.acquire()
       
      item = buffer[out_index]
      out_index = (out_index + 1)%capacity

      msg_no += 1 
      combined_message += item

      if msg_no >= self.combined_message_count:
          #send_message to the messaging service 
        # SqsProducerKombu.send_message_SQS(combined_message)
        print(f'consumed message:{combined_message},{msg_consumed}')
        combined_message = ""
        msg_no = 0
      
      mutex.release()
      empty_cells.release()      
       
      time.sleep(2)
       
      msg_consumed += 1
    
# Creating Threads
start_time = time.time()
producer = Producer()
consumer = Consumer(1)
 
# Starting Threads
producer.start()
consumer.start()

# Waiting for threads to complete
producer.join()
consumer.join()

print(time.time() - start_time)




  