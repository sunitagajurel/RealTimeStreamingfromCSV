import threading 
import time 
import SqsProducerKombu

# Producer Thread Class

msg_no = 0 
combined_message_count = 1
msg_consumed= 0
combined_message = ""
start_time = time.time()
import codecs

try:
    #using codecs to handle to non-unicode symbols in the file
    with open(r"{filepath}",encode = 'utf-8') as csv_file:
        for line in csv_file:
            print(f'published_message{line}')
            msg_no += 1 
            combined_message += line

            if msg_no >= combined_message_count:
                #send_message to the messaging service 
                # SqsProducerKombu.send_message_SQS(combined_message)
                print(f'consumed message:{combined_message},{msg_consumed}')
                combined_message = ""
                msg_no = 0
            
            msg_consumed += 1
except Exception as e: 
    print(e)

print(time.time() - start_time)




        
        