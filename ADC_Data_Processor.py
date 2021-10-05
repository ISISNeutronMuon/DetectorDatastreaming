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

def Packet_Preprocessing(Packet_Data):  #Split each packet into its frames
    Processed_Packet = Packet_Data.replace(HEADER_STRING, ":") #replace all occurances of the frame header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packet = Processed_Packet.replace(END_HEADER, ":") #replace all occurances of the frame END_header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packets = Processed_Packet.split(":") #Split the packet into a list, where each ":" has occured
    Processed_Packets.pop(0) #remove the first packet as its always empty
    return Packet_Data

def HeaderProcessor(Packet_ToProcess): # extracts header data and returns the output

    PacketHeader = Packet_ToProcess[0:128]              #Extract the first 128 Chars from packet - this is the header part
    binary_header = bin(int(PacketHeader, base=16))     #Convert the Hex Header into binary for data extraction

    # Temp Debug additions
   # print("Packet Header: ",PacketHeader)
   # print("Header as binary: ",binary_header)

    Days_STR = binary_header[136:145]       # Extract binary from header for FrameTime - Days
    FrameTime_Days=int(Days_STR,2)          # Convert binary extract into int

    Hours_STR = binary_header[145:150]      # Extract binary from header for FrameTime - Hours
    FrameTime_Hours=int(Hours_STR,2)        # Convert binary extract into int

    Mins_STR = binary_header[150:156]       # Extract binary from header for FrameTime - Mins
    FrameTime_Mins=int(Mins_STR,2)          # Convert binary extract into int

    Secs_STR = binary_header[156:162]       # Extract binary from header for FrameTime - Secs
    FrameTime_Secs=int(Secs_STR,2)          # Convert binary extract into int

    mS_STR = binary_header[164:174]         # Extract binary from header for FrameTime - mS
    FrameTime_mS=int(mS_STR,2)              # Convert binary extract into int

    uS_STR = binary_header[174:184]         # Extract binary from header for FrameTime - uS
    FrameTime_uS=int(uS_STR,2)              # Convert binary extract into int

    nS_STR = binary_header[194:204]         # Extract binary from header for FrameTime - nS
    FrameTime_nS=int(nS_STR,2)
    # print("FrameTime Info: ",FrameTime_Days,":",FrameTime_Hours,":",FrameTime_Mins,":",FrameTime_Secs,":",FrameTime_mS,":",FrameTime_uS,":",FrameTime_nS)




    FrameTime = 0 # time (since Epoch in NS) of the frame event, calculated from each part of the time.


    PeriodNumber = Packet_Data[0:0] # period number for the frame data
    FrameLength = 0  # length of the frame data within this packet
    FrameNumber = 0  # number of this frame

    FrameTime = 0  # time (since Epoch in NS) of the frame event

    return FrameNumber, FrameLength, FrameTime, PeriodNumber

def PacketProcessor_MAPS(Packet_Data, WiringTable): #Pulls the event data out of the streamed packets
    #Note this function only gets MAPs Data from the event packet - Time from TOF, PulseHeight, Position

    return

def GroupPacketByIP(Packet_IPAddress, Packet_Contents):
    ip_data = {"IP_Address":"","Packet_Data":""}
    for i in range(len(Packet_IPAddress)):
        if Packet_IPAddress[i] in ip_data["IP_Address"]:

    return ip_data

def WiringTableReader(CSV_File_Location): #Reads in the CSV containing wiring table information
    IP_Add_Info = {'ADC_REF': "", 'Stream_IP': "",'CH':"",'Detector_ID':"",'Detector_Len':""}

    return IP_Add_Info

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



start_time = datetime.datetime(year=2021, month=9, day=29, hour=16, minute=0)
end_time = datetime.datetime(year=2021, month=9, day=29, hour=17, minute=0)

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

for i in range(len(Packet_Data)):
    HeaderProcessor(Packet_Data[i])

print("Header Process Time:",datetime.datetime.now() - debugt_start)








#for i in range(0, len(Packet_Data)):
#    print("Packet IP:" , Packet_SourceIP[i], " data:", Packet_Data[i])
#print(ip_data[1])
#ch_list = list(range(0,24,1))


#data_split_dict_channel_ip_combine(ip_data,ch_list)
#print("Line Decode Time:",datetime.datetime.now() - debugt_start)
#debugt_start = datetime.datetime.now()
#Flatbuffertestdata = Serialise_EV42_ISIS_Data(4,1,1.35,"MAPs_DECT_C1-PK1-T4",3467,100,[1,1,1,1,1,1,2],[100,200,300,400,500,600,700])
#kafka_helper.send_flatBuffer(Flatbuffertestdata,"MAPS")
#print("Flatbuffer Data send to Kafka: ", Flatbuffertestdata)



