import socket
import data_proc_func
import kafka_helper
import matplotlib.pyplot as plt
import ADC_Data_Processor
import Send_Kafka_Event

HEADER_STRING = "ffffffffffffffff0"
END_HEADER = "efffffffffffffff0"

HOST_PORT = 48641, 48642, 48643, 48644, 48868
HOST_IP = "192.168.1.125"
SRC_IP_ADDRESS = "192.168.1.201", "192.168.1.202", "192.168.1.203", "192.168.1.204", "192.168.1.251"

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
                # print(data.hex())
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


def thread_ch_list(ch_list):
    thread_list = []
    for i in ch_list:
        if i <= 5:
            thread_list.append(0)
        elif 6 <= i <= 11:
            thread_list.append(1)
        elif 12 <= i <= 17:
            thread_list.append(2)
        elif 18 <= i <= 23:
            thread_list.append(3)
    thread_list = set(list(dict.fromkeys(thread_list)))
    return thread_list


def kafka_slim_single_thread_udp_receiver(stop, PACKET_COUNT,instance):  # ,stream_length):
    sock = socket.socket(socket.AF_INET,  # Internet
                         socket.SOCK_DGRAM)  # UDP
    sock.bind((HOST_IP, HOST_PORT[instance],))
    sock.settimeout(2)
    print("Thread", instance, "started")
    while True:
        try:
            if socket.gethostbyname(SRC_IP_ADDRESS[instance]):
                data, addr = sock.recvfrom(900400)
                # ^ set buffer size (did have at 9004 for frame size, not sure if larger helps)
                PACKET_COUNT = PACKET_COUNT + 1
                print("FPGA" + str(instance) + " Packet Count:" + str(PACKET_COUNT))
                # print(data.hex())
                kafka_helper.send_data({'packet': data.hex(), 'packet_info': SRC_IP_ADDRESS[instance]})
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

def data_plot_string(kafka_data):
    position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData, Time = [], [], [], [], [], [], []
    position_combined, PulseHeight_combined, StartSig_combined, Misplace_combined, MaxSlope_combined, AreaData_combined, Time_combined = [], [], [], [], [], [], []
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[128:] for e in kafka_data]  # remove the header from the list
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


def kafka_slim_single_thread_udp_receiver_MultiMerlin(stop, PACKET_COUNT,instance, Stream_Port, Stream_IP):  # ,stream_length):
    sock = socket.socket(socket.AF_INET,  # Internet
                         socket.SOCK_DGRAM)  # UDP
    sock.bind((HOST_IP, Stream_Port,))
    sock.settimeout(2)
    print("Thread", instance, "started")
    while True:
        try:
            if socket.gethostbyname(Stream_IP):
                data, addr = sock.recvfrom(900400)
                # ^ set buffer size (did have at 9004 for frame size, not sure if larger helps)
                PACKET_COUNT = PACKET_COUNT + 1
                print("FPGA" + str(Stream_IP) + " Packet Count:" + str(PACKET_COUNT))
                # print(data.hex())
                # kafka_helper.send_data({'packet': data.hex(), 'packet_info': Stream_IP})
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


def MultipleStreamToProcessedEV42(stop, PACKET_COUNT,instance, Stream_Port, Stream_IP):  # ,stream_length):
    sock = socket.socket(socket.AF_INET,  # Internet
                         socket.SOCK_DGRAM)  # UDP
    sock.bind((HOST_IP, Stream_Port,))
    sock.settimeout(2)
    print("Thread", instance, "started")
    totalnumprocessedevents = 0
    totalnumerror = 0
    while True:
        try:
            if socket.gethostbyname(Stream_IP):
                data, addr = sock.recvfrom(900400)
                # ^ set buffer size (did have at 9004 for frame size, not sure if larger helps)
                PACKET_COUNT = int(PACKET_COUNT) + 1
                # print("FPGA" + str(Stream_IP) + " Packet Count:" + str(PACKET_COUNT))

                PacketFrames = ADC_Data_Processor.PacketFrameSplitter(data.hex())

                # for each of the frames in the current packet:
                for f in range(0, len(PacketFrames)):
                    HeaderData = ADC_Data_Processor.HeaderProcessor(PacketFrames[f])  # process header data - returns list
                    EventData = PacketFrames[f][128:len(PacketFrames[f])]  # Define event data (framedata - header)

                    # process the frame into events

                    result = ADC_Data_Processor.PacketProcessor_MAPS(EventData, Stream_IP)

                    # push the data into ESS Flatbuffer format
                    EV42_FrameData = ADC_Data_Processor.Serialise_EV42(Stream_IP, HeaderData[0], HeaderData[2], result[0],
                                                        result[1])
                    Send_Kafka_Event.send_flatBuffer(EV42_FrameData)
                    totalnumerror += result[5]  # get packet processor number of errors
                    totalnumprocessedevents += int(result[6])  # get packet processor number of events
                print("Thread:", instance, ", SRC IP:", Stream_IP,", Total Event Errors: ",totalnumerror,", Total Events: ", totalnumprocessedevents)
                # print(data.hex())
                # kafka_helper.send_data({'packet': data.hex(), 'packet_info': Stream_IP})
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


# MultipleStreamToProcessedEV42(False, 0, 1 ,48640,"192.168.1.201")