import threading
import udp_functions
stop_threads = False
PACKET_COUNT = 0, 0, 0, 0, 0
##########code to start sending data streams to kafka streams###################################
# thread_list = udp_functions.thread_ch_list(ch_list)
for i in [0, 1, 2, 3]:
    udp_thread = threading.Thread(target=udp_functions.kafka_slim_single_thread_udp_receiver,
                                  args=((lambda: stop_threads), PACKET_COUNT[i], i))
    udp_thread.start()
input("Streams starting, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
stop_threads = True
while True:
    if (udp_thread.is_alive() == False):
        print("threads ended")
        break
