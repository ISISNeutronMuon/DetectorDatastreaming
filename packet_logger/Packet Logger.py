# UDP_threaded_receiver
import os
import sys
import socket
import threading
# import queue
import time

import udp_functions

# import time

# FILE_PATH = os.path.abspath(os.path.dirname(__file__))
# FILE_PATH = os.path.join(FILE_PATH,"../data/")

#FILE_PATH = sys.argv[7]
FILE_PATH = 'I:/LabVIEW/DJT/ADC_data_analysis/data'
PACKET_COUNT = 0, 0, 0, 0
HOST_PORT = 48724, 48725, 48726, 48727
HOST_IP = "192.168.1.125"
IP_ADDRESS = "192.168.2.184", "192.168.2.185", "192.168.2.186", "192.168.2.187"
FILES = FILE_PATH + '/184_packets.txt', \
        FILE_PATH + '/185_packets.txt', \
        FILE_PATH + '/186_packets.txt', \
        FILE_PATH + '/187_packets.txt'

FORTMATED_FILES = FILE_PATH + '/201_thread_formated.txt', \
                  FILE_PATH + '/202_thread_formated.txt', \
                  FILE_PATH + '/203_thread_formated.txt', \
                  FILE_PATH + '/204_thread_formated.txt'

stop_threads = False


udp_0_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[0],
                                                                                       HOST_PORT[0], FILES[0],
                                                                                       (lambda: stop_threads),
                                                                                       PACKET_COUNT[0], 0))
udp_0_thread.start()
time.sleep(1)
udp_1_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[1],
                                                                                       HOST_PORT[1], FILES[1],
                                                                                       (lambda: stop_threads),
                                                                                       PACKET_COUNT[1], 1))
udp_1_thread.start()
time.sleep(1)
udp_2_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[2],
                                                                                       HOST_PORT[2], FILES[2],
                                                                                       (lambda: stop_threads),
                                                                                       PACKET_COUNT[2], 2))
udp_2_thread.start()
time.sleep(1)
udp_3_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[3],
                                                                                       HOST_PORT[3], FILES[3],
                                                                                       (lambda: stop_threads),
                                                                                       PACKET_COUNT[3], 3))
udp_3_thread.start()
time.sleep(1)

input("Streams starting, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
stop_threads = True
while True:
    if (udp_0_thread.is_alive() == False):
        print("thread 0 ended")
        break
while True:
    if (udp_1_thread.is_alive() == False):
        print("thread 1 ended")
        break
while True:
    if (udp_2_thread.is_alive() == False):
        print("thread 2 ended")
        break
while True:
    if (udp_3_thread.is_alive() == False):
        print("thread 3 ended")
        break

    udp_functions.stream_decoder(FILES[0], FORTMATED_FILES[0], 0)
    udp_functions.stream_decoder(FILES[1], FORTMATED_FILES[1], 1)
    udp_functions.stream_decoder(FILES[2], FORTMATED_FILES[2], 2)
    udp_functions.stream_decoder(FILES[3], FORTMATED_FILES[3], 3)

exit()
