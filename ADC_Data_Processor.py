import cProfile
import pstats

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

    Years_STR = binary_header[128:136] # Extract binary from header for FrameTime - Years
    FrameTime_Years = int(Years_STR, 2) # Convert binary extract into int

    Days_STR = binary_header[136:145]  # Extract binary from header for FrameTime - Days
    FrameTime_Days = int(Days_STR, 2)  # Convert binary extract into int

    Hours_STR = binary_header[145:150]  # Extract binary from header for FrameTime - Hours
    FrameTime_Hours = int(Hours_STR, 2)  # Convert binary extract into int

    Mins_STR = binary_header[150:156]  # Extract binary from header for FrameTime - Mins
    FrameTime_Mins = int(Mins_STR, 2)  # Convert binary extract into int

    Secs_STR = binary_header[156:162]  # Extract binary from header for FrameTime - Secs
    FrameTime_Secs = int(Secs_STR, 2)  # Convert binary extract into int

    mS_STR = binary_header[162:172]  # Extract binary from header for FrameTime - mS
    FrameTime_mS = int(mS_STR, 2)  # Convert binary extract into int

    uS_STR = binary_header[172:182]  # Extract binary from header for FrameTime - uS
    FrameTime_uS = int(uS_STR, 2)  # Convert binary extract into int

    nS_STR = binary_header[182:192]  # Extract binary from header for FrameTime - nS
    FrameTime_nS = int(nS_STR, 2)
    # print("FrameTime Info: ",FrameTime_Years, ":",FrameTime_Days,":",FrameTime_Hours,":",FrameTime_Mins,":",FrameTime_Secs,":",FrameTime_mS,":",FrameTime_uS,":",FrameTime_nS)

    # Convert streamed date into nS Since Epoch
    FrameTime = (365 * 8.64e+13) * FrameTime_Years + 30  # add nS since epoch to this year - streamed out is years past 2000 so add 30 for past epoch
    FrameTime += (FrameTime_Days * 8.64e+13)
    FrameTime += (FrameTime_Hours * 3.6e+12)
    FrameTime += (FrameTime_Mins * 6e+10)
    FrameTime += (FrameTime_Secs * 1e+9)
    FrameTime += (FrameTime_mS * 1000000)
    FrameTime += (FrameTime_uS * 1000)
    FrameTime += FrameTime_nS
    FrameTime = int(FrameTime)

    PeriodNo_STR = binary_header[208:224]
    PeriodNumber = int(PeriodNo_STR, 2)

    FrameNo_STR = binary_header[96:128]
    FrameNumber = int(FrameNo_STR, 2)

    EventsInFrame_STR = binary_header[225:256]
    EventsInFrame = int(EventsInFrame_STR, 2)

    # print("Frame No: ",FrameNumber," Events in Frame: ", EventsInFrame, " Frame Time: ", FrameTime," Period No:", PeriodNumber)

    return FrameNumber, EventsInFrame, FrameTime, PeriodNumber, binary_header

def PacketProcessor_MAPS(Packet_Data, numerror , numprocessedevents, SRC_IP):  # Pulls the event data out of the streamed packets
    # Note this function only gets MAPs Data from the event packet - Time from TOF, Position and pulse height

    Frame_Posisitions = []
    Frame_PulHs = []
    Frame_PosOverFlows =[]
    Frame_PulHOverFlows =[]
    Frame_Times = []

    CH_MantidDectID = []
    CH_MantidDectLen = []
    IPWiringTable = df_WiringTable.loc[(df_WiringTable['StreamingIP'] == SRC_IP)]

    for Channel in range(6):
        CH_MantidDectID.append(IPWiringTable['Mantid_DetectorID_Start'].iloc[Channel])
        CH_MantidDectLen.append(IPWiringTable['Mantid_Detector_ID_Lenght'].iloc[Channel])

    numEvents = int((len(Packet_Data) / 16)-1) # work out the number of events in the packet by dividing 16 (event lenght) - and subtracting one for loop
    for event in range(0, numEvents):       # For each event in the packet
        EventStartADD = (event * 16)        # calculate the start address of the event data
        EventEndADD = EventStartADD + 16    # calculate the end address of the event data

        hexEvent = Packet_Data[EventStartADD:EventEndADD]   # Get the hex data from packet between the start/end of event
        binEvent = bin(int(hexEvent, base=16))[2:]          # convert hex into binary - remove first two chars as they are always 0b
        if hexEvent[0:2] == "e0" and len(binEvent) == 64:   # if packet starts with e0 and has correct len
            binEvent = bin(int(hexEvent, base=16))[2:]

            PulHOverflow = binEvent[38:39]
            PosOverflow = binEvent[39:40]

            intTimenS = int(binEvent[8:32], 2)  # convert binary frame time of event into an int
            intPos = int(binEvent[52:64], 2)     # convert binary position of the event into an int
            intPulH = int(binEvent[40:52], 2)    # convert binary pulse height of the event into an int
            intCH = int(binEvent[36:38], 2)        # convert binary channel of the event into an int

            scaledpos = int(intPos / (4096 / CH_MantidDectLen[intCH]))  # Scale the event to the mantid tube lenght - 4096 is the range of streamed output
            Mantid_Pixel = scaledpos + CH_MantidDectID[intCH]       # Add to Mantid Dect ID start to move to the correct mantid tube

            # append each events data to lists
            Frame_Posisitions.append(Mantid_Pixel)
            Frame_Times.append(intTimenS)
            Frame_PulHs.append(intPulH)
            Frame_PosOverFlows.append(PosOverflow)
            Frame_PulHOverFlows.append(PulHOverflow)

            numprocessedevents += 1
        else:
            numerror += 1
            print("INVALID EVENT DETECTED, Error: ", numerror, " Bad Event: ", hexEvent, " Error SRC IP: ", SRC_IP)

  #   print("Frame Times List: ", Frame_Times)
  #   print("Event Positions: ", Frame_Posisitions)
  #   print("Frame PulH: ", Frame_PulHs)
  #   print("Frame Position Overflows: ", Frame_PosOverFlows)
  #   print("Frame PulH Overflows: ", Frame_PulHOverFlows)
    return Frame_Times, Frame_Posisitions, Frame_PulHs, Frame_PulHOverFlows, Frame_PosOverFlows, numerror, numprocessedevents


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


#start_time = datetime.datetime(year=2021, month=10, day=15, hour=10, minute=46)   # Data Collection start time
start_time = datetime.datetime(year=2021, month=10, day=20, hour=12, minute=30)   # Data Collection start time
#end_time = datetime.datetime(year=2021, month=10, day=18, hour=16, minute=1)
end_time = datetime.datetime.now()
print("Data Start Time: ", start_time, " Data End Time: ", end_time)

# Collect Kafka Data
collect_start = datetime.datetime.now()                         # start perf timer
all_data = kafka_helper.get_data_between(start_time, end_time)  # get kafka data from all MADC's for a given time range
list_length = len(all_data)                                     # print the overall lenght of the data - number of packets
print("Number of Packets to Process: ", list_length)            # print info for user
collect_time = datetime.datetime.now() - collect_start          # Calc total collection time

# Read in instrument DAES wiring table
df_WiringTable = WiringTableReader('DAES_WiringTable_MAPS.csv')

numerror = 0            # Couter for number of events that reported errors
numprocessedevents = 0  # Counter for number of processed events
TotalPacketCount = 0    # Counter for total number of packets

loopstart = datetime.datetime.now() # Get time of processing start

#Process Historic Kafka Data - run for each packet in the kafka data
for packet in range(0, len(all_data)):
    PacketFrames = Packet_Preprocessing(all_data[packet].get("packet")) # split the packet into list of frames
    TotalPacketCount += len(PacketFrames)                                    # Add number of frame to running total
    CurrentFrameSRC = str(all_data[packet].get("packet_info"))          # Get Current Packets source IP

    # for each of the frames in the current packet:
    for f in range(0, len(PacketFrames)):
        HeaderData = HeaderProcessor(PacketFrames[f])               # process header data - returns list
        EventData = PacketFrames[f][128:len(PacketFrames[f])]       # Define event data (framedata - header)

        # process the frame into events
        result = PacketProcessor_MAPS(EventData, numerror, numprocessedevents,
                                      all_data[packet].get("packet_info"))

        # push the data into ESS Flatbuffer format
        EV42_FrameData = Serialise_EV42(CurrentFrameSRC, numprocessedevents, HeaderData[2], result[0], result[1])

        #
        numerror = result[5]                    # get packet processor number of errors
        numprocessedevents = int(result[6])     # get packet processor number of events

currenttime = datetime.datetime.now()           #get processing complete time
overallrun = currenttime - collect_start        #Calculate overall runtime
processingtime = currenttime - loopstart        #calculate runtime

print("ADC Data Processor Runtime Statistics: - header --> Packet")
print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
print("Kafka Collection Time: ", collect_time)
print("")
print("Started process: ", loopstart, ", Processing finished: ", currenttime)
print("Processing Time: ", processingtime)
print("")
print("Total Frame Processing Rate: ", TotalPacketCount / processingtime.total_seconds(), " (Frames Per Second)")
print("Total Event Processing Rate: ", numprocessedevents / processingtime.total_seconds(), " (Events Per Second)")









#stats = pstats.Stats(pr)
#stats.sort_stats(pstats.SortKey.TIME)
#now = datetime.datetime.now()
#profilename = "ProfilingEventHandler"+ now.strftime("D%d_M%m_Y%Y_H%H_M%M_S%S") + ".prof"
#stats.dump_stats(filename=profilename)


# print("")
# now = datetime.datetime.now()
# csvfilename = "Frames_" + now.strftime("D%d_M%m_Y%Y_H%H_M%M_S%S") + ".csv"
# print("Saving main DataFrame as: ", csvfilename)
# df_Frames.to_csv(csvfilename)
# print("COMPLETE")