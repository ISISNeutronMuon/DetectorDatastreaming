import kafka_helper     # import Kafka Help - used to pull and push data
from streaming_data_types.eventdata_ev42 import serialise_ev42, deserialise_ev42 # import ESS Flatbuffer serialiser
import datetime         # import datetime for performance information
import csv
import pandas as pd

# set Pandas display configs
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

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
                        # print(int(int(list_line[i + 29:i + 32], 16) / 16))


def Packet_Preprocessing(Packet_Data):  # Split each packet into its frames
    Processed_Packet = Packet_Data.replace(HEADER_STRING,
                                           ":" + HEADER_STRING)  # replace all occurances of the frame header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packet = Processed_Packet.replace(END_HEADER,
                                                ":" + END_HEADER)  # replace all occurances of the frame END_header string in the packet with ":", this will tell us where to split the packet later on
    Processed_Packets = Processed_Packet.split(":")  # Split the packet into a list, where each ":" has occured
    Processed_Packets.pop(0)  # remove the first packet as its always empty
    return Processed_Packets

def HeaderProcessor(Packet_ToProcess):  # extracts header data and returns the output

    PacketHeader = Packet_ToProcess[0:128]  # Extract the first 128 Chars from packet - this is the header part
    binary_header = bin(int(PacketHeader, base=16))[2:]  # Convert the Hex Header into binary for data extraction, remove the first two chars to remove "0b" infront of string

    Days_STR = binary_header[134:143]  # Extract binary from header for FrameTime - Days
    FrameTime_Days = int(Days_STR, 2)  # Convert binary extract into int

    Hours_STR = binary_header[143:148]  # Extract binary from header for FrameTime - Hours
    FrameTime_Hours = int(Hours_STR, 2)  # Convert binary extract into int

    Mins_STR = binary_header[148:154]  # Extract binary from header for FrameTime - Mins
    FrameTime_Mins = int(Mins_STR, 2)  # Convert binary extract into int

    Secs_STR = binary_header[154:160]  # Extract binary from header for FrameTime - Secs
    FrameTime_Secs = int(Secs_STR, 2)  # Convert binary extract into int

    mS_STR = binary_header[162:172]  # Extract binary from header for FrameTime - mS
    FrameTime_mS = int(mS_STR, 2)  # Convert binary extract into int

    uS_STR = binary_header[172:182]  # Extract binary from header for FrameTime - uS
    FrameTime_uS = int(uS_STR, 2)  # Convert binary extract into int

    nS_STR = binary_header[182:192]  # Extract binary from header for FrameTime - nS
    FrameTime_nS = int(nS_STR, 2)
    # print("FrameTime Info: ",FrameTime_Days,":",FrameTime_Hours,":",FrameTime_Mins,":",FrameTime_Secs,":",FrameTime_mS,":",FrameTime_uS,":",FrameTime_nS)

    # Convert streamed date into nS Since Epoch
    FrameTime = (365 * 8.64e+13) * 51  # add nS since epoch to this year - not currently streamed out
    FrameTime = FrameTime + (FrameTime_Days * 8.64e+13)
    FrameTime = FrameTime + (FrameTime_Hours * 3.6e+12)
    FrameTime = FrameTime + (FrameTime_Mins * 6e+10)
    FrameTime = FrameTime + (FrameTime_Secs * 1e+9)
    FrameTime = FrameTime + (FrameTime_mS * 1000000)
    FrameTime = FrameTime + (FrameTime_uS * 1000)
    FrameTime = FrameTime + FrameTime_nS

    PeriodNo_STR = binary_header[208:224]
    PeriodNumber = int(PeriodNo_STR, 2)

    FrameNo_STR = binary_header[96:128]
    FrameNumber = int(FrameNo_STR, 2)

    EventsInFrame_STR = binary_header[225:256]
    EventsInFrame = int(EventsInFrame_STR, 2)

    #  print("Frame No: ",FrameNumber," Frame Len: ", FrameLength, " Frame Time: ", FrameTime," Period No:", PeriodNumber)

    return FrameNumber, EventsInFrame, FrameTime, PeriodNumber, binary_header


def PacketProcessor_MAPS(Packet_Data, WiringTable, SourceIP, numerror , numprocessedevents, SRC_IP, alldata):  # Pulls the event data out of the streamed packets
    # Note this function only gets MAPs Data from the event packet - Time from TOF, Position and pulse height

    numEvents = int((len(Packet_Data) / 16)-1) # work out the number of events in the packet by dividing 16 (event lenght) - and subtracting one for loop
    for event in range(0, numEvents):       # For each event in the packet
        EventStartADD = (event * 16)        # calculate the start address of the event data
        EventEndADD = EventStartADD + 16    # calculate the end address of the event data

        hexEvent = Packet_Data[EventStartADD:EventEndADD]   # Get the hex data from packet between the start/end of event
        binEvent = bin(int(hexEvent, base=16))[2:]          # convert hex into binary - remove first two chars as they are always 0b
        if hexEvent[0:2] == "e0" and len(binEvent) == 64:   # if packet starts with e0 and has correct len
            binEvent = bin(int(hexEvent, base=16))[2:]
            binTime = binEvent[8:32]     # extract frame time of event - binary
            binPos = binEvent[52:64]     # extract position of event - binary
            binPulH = binEvent[40:52]    # extract pulse height since TOF of event - binary
            binCH = binEvent[36:38]      # extract FPGA Channel of event - binary

            PulHOverflow = binEvent[38:39]
            PosOverflow = binEvent[39:40]

            intTimenS = int(binTime, 2)     # convert binary frame time of event into an int
            intPos = int(binPos, 2)     # convert binary position of the event into an int
            intPulH = int(binPulH, 2)    # convert binary pulse height of the event into an int
            intCH = int(binCH, 2)        # convert binary channel of the event into an int

            numprocessedevents = numprocessedevents + 1
        else:
            numerror = numerror + 1
            print("INVALID EVENT DETECTED, Error: ", numerror, " Bad Event: ", hexEvent, " Error SRC IP: ", SRC_IP)
    return numerror, numprocessedevents


def WiringTableReader(CSV_File_Location):  # Reads in the CSV containing wiring table information
    WiringTablePD = pd.read_csv(CSV_File_Location)
    return WiringTablePD


def Serialise_EV42_ISIS_Data(period_number, run_state, proton_charge, source_name, message_id, pulse_time,
                             time_of_flight, detector_id):
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

def Serialise_EV42(source_name, message_id, pulse_time, time_of_flight, detector_id):
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




start_time = datetime.datetime(year=2021, month=10, day=15, hour=10, minute=46)   # Data Collection start time
#end_time = datetime.datetime(year=2021, month=10, day=7, hour=11, minute=1)
end_time = datetime.datetime.now()


print("Data Start Time: ", start_time, " Data End Time: ", end_time)

collect_start = datetime.datetime.now()
all_data = kafka_helper.get_data_between(start_time, end_time)  # get kafka data from all MADC's for a given time range
list_length = len(all_data)  # print the overall lenght of the data - number of packets
print("Number of Packets to Process: ", list_length)

collect_time = datetime.datetime.now() - collect_start

HeaderProcess_start = datetime.datetime.now()

df_Frames = pd.DataFrame(columns=["SRC IP",
                                  "FrameData",
                                  "FrameNumber",
                                  "EventsInFrame",
                                  "FrameTime",
                                  "PeriodNumber",
                                  "FrameRawEventData",
                                  "binaryHeader"])

All_Frame_Info = {}

frameNumber=0
for p in range(0, len(all_data)):  # for each packet (p)
    PacketFrames = Packet_Preprocessing(all_data[p].get("packet"))

    #print(len(PacketFrames))
    for f in range(0, len(PacketFrames)): #for each frame (f) in each packet
        HeaderData = HeaderProcessor(PacketFrames[f])  # process header data - returns list
        EventData = PacketFrames[f][128:len(PacketFrames[f])]

        All_Frame_Info[frameNumber] = {  # create dict with all packet data
            'SRC IP': all_data[p].get("packet_info"),
            'FrameData': PacketFrames[f],
            'FrameNumber': HeaderData[0],
            'EventsInFrame': HeaderData[1],
            'FrameTime': HeaderData[2],
            'PeriodNumber': HeaderData[3],
            'binaryHeader': HeaderData[4],
            'FrameRawEventData': EventData}
        frameNumber=frameNumber+1

df_Frames = pd.DataFrame.from_dict(All_Frame_Info, orient='index')

HeaderProcess_time = datetime.datetime.now() - HeaderProcess_start

print("Headers Processed")

df_WiringTable = WiringTableReader('DAES_WiringTable_MAPS.csv')

event_timeS = datetime.datetime.now()
numprocessedevents = 0
numerror = 0
for i in range(0, len(df_Frames)):
        result = PacketProcessor_MAPS(df_Frames.at[i, 'FrameRawEventData'], df_WiringTable, df_Frames.at[i,'SRC IP'], numerror, numprocessedevents,df_Frames.at[i, 'SRC IP'],df_Frames.at[i, 'FrameData'])
        numerror = result[0]
        numprocessedevents = result[1]

event_Runtime = datetime.datetime.now() - event_timeS
currenttime = datetime.datetime.now()
overallrun = currenttime - collect_start

print("ADC Data Processor Runtime Statistics:")
print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
print("Kafka Collection Time: ", collect_time)
print("")
print("Header Process Time: ", HeaderProcess_time)
print("df created, number of items:", len(df_Frames))
print("Processing rate (Frames Per Second): ", (len(df_Frames)) / HeaderProcess_time.total_seconds())
print("")
print("Event Processing time: ", event_Runtime , " Processed Events: ", numprocessedevents)
print("Processing Rate: ", numprocessedevents/event_Runtime.total_seconds(), " (Events/S)")
print("")
print("Overall Run Stats:")
print("Total Runtime: ", currenttime - collect_start)
print("Number of Packets: ", list_length)
print("Number of Frames: ", len(df_Frames))
print("Number of events: ", numprocessedevents)

print("Total Frame Processing Rate: ", (len(df_Frames)) / overallrun.total_seconds(), " (Frames Per Second)")
print("Total Event Processing Rate: ", numprocessedevents / overallrun.total_seconds(), " (Events Per Second)")

print("Frame info: ")
print(df_Frames.info())

print("")
print("Saving main DataFrame as: FrameCapture.csv")
df_Frames.to_csv('FrameCaptures.csv')
print("COMPLETE")