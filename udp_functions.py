# single_thread_udp_receiver
import collections
import os
import sys
import socket
import kafka_helper
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import time
import numpy as np


# kafka_data_dict = {'position':[], 'PulseHeight':[],'StartSig':[],'Misplace':[],'MaxSlope':[],'AreaData':[]}

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


def kafka_single_thread_udp_receiver(HOST_IP, SRC_IP_ADDRESS, HOST_PORT, stop, PACKET_COUNT,
                                     instance):  # ,stream_length):
    PACKET_COUNT = 0
    # HOST_IP = "192.168.1.119"
    IP_ADDRESS_0 = SRC_IP_ADDRESS
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
                kafka_helper.send_data({'packet': data.hex(), 'packet_info': SRC_IP_ADDRESS})
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


def kafka_frame_decoder(start_time, end_time):
    HEADER_STRING = "ffffffffffffffff0"
    END_HEADER = "efffffffffffffff0"
    processed_data = ""
    current_processed_data = ""
    i = 0
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    for i in range(0, list_length):
        current_processed_data = (all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        processed_data = processed_data + current_processed_data
    return processed_data


def kafka_frame_decoder_ip(start_time, end_time):
    HEADER_STRING = "ffffffffffffffff0"
    END_HEADER = "efffffffffffffff0"
    processed_data = {'packet':"", 'packet_info':""}
    current_processed_data = {'packet':"", 'packet_info':""}
    i = 0
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    for i in range(0, list_length):
        current_processed_data['packet'] = (all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        current_processed_data['packet_info'] = all_data[i].get("packet_info")
        processed_data.update(current_processed_data)
    return processed_data

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


def data_split(kafka_data):
    position = []
    PulseHeight = []
    StartSig = []
    Misplace = []
    MaxSlope = []
    AreaData = []
    Time = []
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[96:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        kafka_data_length = (len(line))
        for i in range(0, kafka_data_length, 32):  # return 32 character chunks
            position.append(int(line[i + 29:i + 32], 16))
            PulseHeight.append(int(line[i + 26:i + 29], 16))
            StartSig.append(int(line[i + 23:i + 26], 16))
            Misplace.append(int(line[i + 20:i + 23], 16))
            MaxSlope.append(int(line[i + 17:i + 20], 16))
            AreaData.append(int(line[i + 14:i + 17], 16))
            Time.append(int(line[i + 3:i + 8], 16))
    return position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData, Time


def dict_data_split(kafka_data):
    position = []
    PulseHeight = []
    StartSig = []
    Misplace = []
    MaxSlope = []
    AreaData = []
    kafka_data_dict = {'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                       'StartSig': collections.Counter(),
                       'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                       'AreaData': collections.Counter()}

    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[96:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        kafka_data_length = (len(line))
        for i in range(0, kafka_data_length, 32):  # return 32 character chunks
            position.append(int(line[i + 29:i + 32], 16))
            PulseHeight.append(int(line[i + 26:i + 29], 16))
            StartSig.append(int(line[i + 23:i + 26], 16))
            Misplace.append(int(line[i + 20:i + 23], 16))
            MaxSlope.append(int(line[i + 17:i + 20], 16))
            AreaData.append(int(line[i + 14:i + 17], 16))
    kafka_data_dict['position'] = collections.Counter(position)
    kafka_data_dict['PulseHeight'] = collections.Counter(PulseHeight)
    kafka_data_dict['StartSig'] = collections.Counter(StartSig)
    kafka_data_dict['Misplace'] = collections.Counter(Misplace)
    kafka_data_dict['MaxSlope'] = collections.Counter(MaxSlope)
    kafka_data_dict['AreaData'] = collections.Counter(AreaData)
    return kafka_data_dict


def dict_add(dicta, dictb):
    for key in dicta:
        if key in dictb:
            dicta[key] = dicta[key] + dictb[key]
        else:
            pass
    return dicta


def data_plot_string(kafka_data):
    position_combined = []
    PulseHeight_combined = []
    StartSig_combined = []
    Misplace_combined = []
    MaxSlope_combined = []
    AreaData_combined = []
    Time_combined = []
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[96:] for e in kafka_data]  # remove the header from the list

    for line in kafka_data:
        position = [(int(line[i + 29:i + 32], 16) / 8) for i in range(0, len(line), 32)]
        PulseHeight = [(int(line[i + 26:i + 29], 16) / 8) for i in range(0, len(line), 32)]
        StartSig = [(int(line[i + 23:i + 26], 16) / 8) for i in range(0, len(line), 32)]
        Misplace = [(int(line[i + 20:i + 23], 16) / 8) for i in range(0, len(line), 32)]
        MaxSlope = [(int(line[i + 17:i + 20], 16) / 8) for i in range(0, len(line), 32)]
        AreaData = [(int(line[i + 14:i + 17], 16) / 8) for i in range(0, len(line), 32)]
        Time = [(int(line[i + 3:i + 8], 16)) for i in range(0, len(line), 32)]

        position_combined = position_combined + position
        PulseHeight_combined = PulseHeight_combined + PulseHeight
        StartSig_combined = StartSig_combined + StartSig
        Misplace_combined = Misplace_combined + Misplace
        MaxSlope_combined = MaxSlope_combined + MaxSlope
        AreaData_combined = AreaData_combined + AreaData
        Time_combined = Time_combined + Time

    figure, axes = plt.subplots(nrows=2, ncols=4)
    axes[0, 0].hist(position_combined, bins=255)
    axes[0, 0].set_title('Position')
    axes[0, 1].hist(PulseHeight_combined, bins=255)
    axes[0, 1].set_title('Pulse Height')
    axes[0, 2].hist(StartSig_combined, bins=255)
    axes[0, 2].set_title('Start Sig')
    axes[1, 0].hist(Misplace_combined, bins=255)
    axes[1, 0].set_title('Misplace')
    axes[1, 1].hist(MaxSlope_combined, bins=255)
    axes[1, 1].set_title('Max Slope')
    axes[1, 2].hist(AreaData_combined, bins=255)
    axes[1, 2].set_title('Area Data')
    axes[0, 3].hist(Time_combined)
    axes[0, 3].set_title('Time')
    plt.show()
    return
