import kafka_helper
from streaming_data_types.eventdata_ev42 import serialise_ev42, deserialise_ev42
import datetime
HEADER_STRING = "ffffffffffffffff0"
END_HEADER = "efffffffffffffff0"

def data_split_dict_channel_ip_combine(kafka_data, ch_list):
    position = [[] for i in range(24)]
    PulseHeight = [[] for i in range(24)]
    StartSig = [[] for i in range(24)]
    Misplace = [[] for i in range(24)]
    MaxSlope = [[] for i in range(24)]
    AreaData = [[] for i in range(24)]
    l = 0

    for line in kafka_data.values():
        channel_offset = (int(
            list(kafka_data.keys())[l][11:]) - 1) * 6  # create and offset for channel based on ipaddress
        l = l + 1
        for list_line in line:
            kafka_data_length = (len(list_line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                if list_line[i:i + 32] != '00000000000000000000000000000000':  # to remove empty packets
                    channel = int(((bin(int('1' + (list_line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
                    if channel in ch_list:
                        donothing = 0
                        #print(int(int(list_line[i + 29:i + 32], 16) / 16))

def Multi_Packet_Preprocessing(all_data):
    current_data = {'packet': "", 'packet_info': ""}
    ip_data = {'packet': "", 'packet_info': ""}
    for i in range(0, 1):
        current_data['packet'] = ((all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
                                  .replace(END_HEADER, "\n" + END_HEADER)) #add "/n" next to each header
        print("Pre Line", current_data['packet'])
        current_data['packet_info'] = all_data[i].get("packet_info")
        current_data['packet'] = current_data.get("packet").splitlines()  # turn carriage return string into a list
        current_data['packet'] = [e[128:] for e in current_data.get("packet")]  # remove the header from the list
        print("Post Line", current_data['packet'])
        if current_data.get("packet_info") in ip_data:
            ip_data[current_data.get("packet_info")] = ip_data[current_data.get("packet_info")] \
                                                       + current_data.get("packet")
        else:
            ip_data[current_data.get("packet_info")] = current_data.get("packet")

    return ip_data

def Packet_Preprocessing(Packet_Data):  #Split each packet into its frames
    #print(Packet_Data)
    Processed_Packet = Packet_Data.replace(HEADER_STRING, ":") #replace all occurances of the frame header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packet = Processed_Packet.replace(END_HEADER, ":") #replace all occurances of the frame END_header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packets = Processed_Packet.split(":") #Split the packet into a list, where each ":" has occured
    Processed_Packets.pop(0) #remove the first packet as its always empty
    return Processed_Packets


def Serialise_EV42_ISIS_Data(period_number, run_state, proton_charge, source_name, message_id,pulse_time, time_of_flight, detector_id):
    """
    Round-trip to check what we serialise is what we get back.
    """
    isis_data = {"period_number": period_number, "run_state": run_state, "proton_charge": proton_charge}

    Event_Data = {
        "source_name": source_name,
        "message_id": message_id,
        "pulse_time": pulse_time,
        "time_of_flight": time_of_flight,
        "detector_id": detector_id,
        "isis_specific": isis_data,
    }

    EV42 = serialise_ev42(**Event_Data)
    return EV42

def Serialise_EV42(source_name, message_id,pulse_time, time_of_flight, detector_id):
    """
    Round-trip to check what we serialise is what we get back.
    """
    Event_Data = {
        "source_name": source_name,
        "message_id": message_id,
        "pulse_time": pulse_time,
        "time_of_flight": time_of_flight,
        "detector_id": detector_id,
    }

    EV42 = serialise_ev42(**Event_Data)
    return EV42

start_time = datetime.datetime(year=2021, month=9, day=23, hour=16, minute=0)
end_time = datetime.datetime(year=2021, month=9, day=23, hour=16, minute=30)

#start_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
#end_time = datetime.datetime.now()
print(start_time)
print(end_time)

debugt_start = datetime.datetime.now()

all_data = kafka_helper.get_data_between(start_time, end_time) #get kafka data from all MADC's for a given time range
list_length = len(all_data) #print the overall lengh of the data - number of packets
print("Number of Packets to Process: ",list_length)
print("Collect time:",datetime.datetime.now() - debugt_start)
debugt_start = datetime.datetime.now()

Packet_Data = []
Packet_SourceIP = []

#list_length = 1
ip_data = {'packet': "", 'packet_info': ""}
for i in range(0, list_length):
    Packet_Data.append(Packet_Preprocessing(all_data[i].get("packet")))
    Packet_SourceIP.append(all_data[i].get("packet_info"))

print("Number of packets processed: ",len(Packet_Data))
print(Packet_Data[0])
print(Packet_SourceIP[0])



print("Loop Time:",datetime.datetime.now() - debugt_start)
debugt_start = datetime.datetime.now()

#for i in range(0, len(Packet_Data)):
#    print("Packet IP:" , Packet_SourceIP[i], " data:", Packet_Data[i])
#print(ip_data[1])
ch_list = list(range(0,24,1))


#data_split_dict_channel_ip_combine(ip_data,ch_list)
print("Line Decode Time:",datetime.datetime.now() - debugt_start)
debugt_start = datetime.datetime.now()
Flatbuffertestdata = Serialise_EV42_ISIS_Data(4,1,1.35,"MAPs_DECT_C1-PK1-T4",3467,100,[1,1,1,1,1,1,2],[100,200,300,400,500,600,700])
#kafka_helper.send_flatBuffer(Flatbuffertestdata,"MAPS")
#print("Flatbuffer Data send to Kafka: ", Flatbuffertestdata)



