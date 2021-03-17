# UDP_threaded_receiver
import os
import sys
import socket
import threading
# import queue
import udp_functions

# import time

# FILE_PATH = os.path.abspath(os.path.dirname(__file__))
# FILE_PATH = os.path.join(FILE_PATH,"../data/")

FILE_PATH = sys.argv[7]
# FILE_PATH = 'I:/LabVIEW/DJT/ADC_data_analysis/data'
PACKET_COUNT = 0, 0, 0, 0, 0
# HOST_PORT = 48788, 48789, 48790, 48795, 48797
# HOST_PORT = 48864, 48865, 48866, 48867, 48868
HOST_PORT = 48641, 48642, 48643, 48644, 48868
HOST_IP = sys.argv[1]
IP_ADDRESS = "192.168.1.201", "192.168.1.202", "192.168.1.203", "192.168.1.204", "192.168.1.251"
FILES = FILE_PATH + '/201_thread.txt', \
        FILE_PATH + '/202_thread.txt', \
        FILE_PATH + '/203_thread.txt', \
        FILE_PATH + '/204_thread.txt', \
        FILE_PATH + '/251_thread.txt'

FORTMATED_FILES = FILE_PATH + '/201_thread_formated.txt', \
                  FILE_PATH + '/202_thread_formated.txt', \
                  FILE_PATH + '/203_thread_formated.txt', \
                  FILE_PATH + '/204_thread_formated.txt', \
                  FILE_PATH + '/251_thread_formated.txt'
stop_threads = False

if (sys.argv[2] == str(1)):
    udp_0_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[0],
                                                                                           HOST_PORT[0], FILES[0],
                                                                                           (lambda: stop_threads),
                                                                                           PACKET_COUNT[0], 0))
    udp_0_thread.start()
if (sys.argv[3] == str(1)):
    udp_1_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[1],
                                                                                           HOST_PORT[1], FILES[1],
                                                                                           (lambda: stop_threads),
                                                                                           PACKET_COUNT[1], 1))
    udp_1_thread.start()
if (sys.argv[4] == str(1)):
    udp_2_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[2],
                                                                                           HOST_PORT[2], FILES[2],
                                                                                           (lambda: stop_threads),
                                                                                           PACKET_COUNT[2], 2))
    udp_2_thread.start()
if (sys.argv[5] == str(1)):
    udp_3_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[3],
                                                                                           HOST_PORT[3], FILES[3],
                                                                                           (lambda: stop_threads),
                                                                                           PACKET_COUNT[3], 3))
    udp_3_thread.start()
if (sys.argv[6] == str(1)):
    udp_4_thread = threading.Thread(target=udp_functions.single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[4],
                                                                                           HOST_PORT[4], FILES[4],
                                                                                           (lambda: stop_threads),
                                                                                           PACKET_COUNT[4], 4))
    udp_4_thread.start()

input("Streams starting, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
stop_threads = True
if sys.argv[2] == str(1):
    while True:
        if (udp_0_thread.is_alive() == False):
            print("thread 0 ended")
            break
if (sys.argv[3] == str(1)):
    while True:
        if (udp_1_thread.is_alive() == False):
            print("thread 1 ended")
            break
if (sys.argv[4] == str(1)):
    while True:
        if (udp_2_thread.is_alive() == False):
            print("thread 2 ended")
            break
if (sys.argv[5] == str(1)):
    while True:
        if (udp_3_thread.is_alive() == False):
            print("thread 3 ended")
            break
if (sys.argv[6] == str(1)):
    while True:
        if (udp_4_thread.is_alive() == False):
            print("thread 4 ended")
            break

if sys.argv[2] == str(1):
    udp_functions.stream_decoder(FILES[0], FORTMATED_FILES[0], 0)
if sys.argv[3] == str(1):
    udp_functions.stream_decoder(FILES[1], FORTMATED_FILES[1], 1)
if sys.argv[4] == str(1):
    udp_functions.stream_decoder(FILES[2], FORTMATED_FILES[2], 2)
if sys.argv[5] == str(1):
    udp_functions.stream_decoder(FILES[3], FORTMATED_FILES[3], 3)
if sys.argv[6] == str(1):
    udp_functions.stream_decoder(FILES[4], FORTMATED_FILES[4], 4)

exit()
