# single_thread_udp_receiver
import os
import sys
import socket


def single_thread_udp_receiver(HOST_IP, SRC_IP_ADDRESS, HOST_PORT, output_file, stop, PACKET_COUNT,
                               instance):  # ,stream_length):
    PACKET_COUNT = 0
    # HOST_IP = "192.168.1.119"
    IP_ADDRESS_0 = SRC_IP_ADDRESS
    FILE0 = open(output_file, "w")
    sock = socket.socket(socket.AF_INET,  # Internet
                         socket.SOCK_DGRAM)  # UDP
    sock.bind((HOST_IP, HOST_PORT,))
    sock.settimeout(2)
    print("Thread", instance, "started")
    while True:
        try:
            if socket.gethostbyname(IP_ADDRESS_0):
                data, addr = sock.recvfrom(
                    900400)  # set buffer size (did have at 9004 for frame size, not sure if larger helps)
                PACKET_COUNT = PACKET_COUNT + 1
                # Update the display
                # print("Packets Received:", PACKET_COUNT,instance,end="\r",flush=True)
                # Save the new data after the old data
                FILE0.write(data.hex() + "\n")
        except socket.timeout:
            if stop():
                # print("thread killed_in")
                break
            continue
            # break
        if stop():
            print("Total Packets Received:", instance, PACKET_COUNT, "\n", end="\r", flush=True)
            # print("thread killed",instance,"\n",end="\r",flush=True)
            break
    return PACKET_COUNT


# single_thread_udp_receiver('192.168.1.148', '48788', 'test.txt', 5)

def stream_decoder(raw_stream, reformatted_stream, instance):
    OLD_FILE0 = open(raw_stream, "r+")
    NEW_FILE0 = open(reformatted_stream, "w+")
    # set files
    HEADER_STRING = "ffffffffffffffff0"
    END_HEADER = "efffffffffffffff0"
    print("formatting", instance)
    while True:
        # Get next line from file
        current_line = OLD_FILE0.readline().rstrip()
        # If line is empty then end of file reached
        if not current_line:
            break
        current_line = current_line.replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        NEW_FILE0.write(current_line)
    # Close files 
    OLD_FILE0.close()
    NEW_FILE0.close()
    return
