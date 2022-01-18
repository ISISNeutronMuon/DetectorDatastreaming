import cProfile
import pstats

import kafka_helper     # import Kafka Help - used to pull and push data
from streaming_data_types.eventdata_ev42 import serialise_ev42 # import ESS Flatbuffer serialiser
import datetime         # import datetime for performance information
import csv



# set Pandas display configs
#pd.set_option('display.max_colwidth', None)
#pd.set_option('display.max_columns', None)
#pd.set_option('display.width', None)

# set times to process between
#start_time = datetime.datetime(year=2021, month=10, day=15, hour=10, minute=46)   # Data Collection start time
start_time = datetime.datetime(year=2021, month=10, day=20, hour=12, minute=30)   # Data Collection start time
#end_time = datetime.datetime(year=2021, month=10, day=18, hour=16, minute=1)
end_time = datetime.datetime.now()

HEADER_STRING = "ffffffffffffffff0"
END_HEADER = "efffffffffffffff0"

def PacketFrameSplitter(Packet_Data):  # Split each packet into its frames
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

    FrameTime_Years = int(binary_header[128:136], 2)    # Extract year's (as int) from header binary
    FrameTime_Days = int(binary_header[136:145], 2)     # Extract Day's (as int) from header binary
    FrameTime_Hours = int(binary_header[145:150], 2)    # Extract Hour's (as int) from header binary
    FrameTime_Mins = int(binary_header[150:156], 2)     # Extract Min's (as int) from header binary
    FrameTime_Secs = int(binary_header[156:162], 2)     # Extract Sec's (as int) from header binary
    FrameTime_mS = int(binary_header[162:172], 2)       # Extract mS's (as int) from header binary
    FrameTime_uS = int(binary_header[172:182], 2)       # Extract uS's (as int) from header binary
    FrameTime_nS = int(binary_header[182:192], 2)       # Extract nS's (as int) from header binary

    # Convert streamed date into nS Since Epoch
    FrameTime = int(((365 * 8.64e+13) * FrameTime_Years + 30) + (FrameTime_Days * 8.64e+13) +
                    (FrameTime_Hours * 3.6e+12) + (FrameTime_Mins * 6e+10) + (FrameTime_Secs * 1e+9) +
                    (FrameTime_mS * 1000000) + (FrameTime_uS * 1000) + FrameTime_nS)

    PeriodNo_STR = binary_header[208:224]
    PeriodNumber = int(PeriodNo_STR, 2)

    PeriodSequence_STR = binary_header[192:207]
    PeriodSequence = int(PeriodSequence_STR, 2)

    FrameNo_STR = binary_header[96:128]
    FrameNumber = int(FrameNo_STR, 2)

    EventsInFrame_STR = binary_header[224:256]
    EventsInFrame = int(EventsInFrame_STR, 2)

    TotalRunPPP_STR = binary_header[256:288]
    TotalRunPPP = int(TotalRunPPP_STR,2)

    veto_frame_data_overrun = False
    veto_frame_mem_full = False
    veto_no_frame_sync = False
    veto_bad_frame = False

    veto_fast_chopper_str = binary_header[307:311]
    veto_External_str = binary_header[311:315]

    veto_fast_chopper = []
    veto_External = []

    for char in veto_fast_chopper_str:
        veto_fast_chopper.append(bool(veto_fast_chopper_str[char]))
    for char in veto_External_str:
        veto_External.append(bool(veto_External_str[char]))

    veto_ISIS_Slow = bool(binary_header[315:316])
    veto_Wrong_Pulse = bool(binary_header[316:317])
    veto_TS2_Pulse = bool(binary_header[317:318])
    veto_SMP = bool(binary_header[318:319])
    veto_FIFO = bool(binary_header[319:320])

    veto_all_bool_vals = []
    veto_all_naming = []

    veto_all_bool_vals.append(veto_ISIS_Slow)
    veto_all_naming.append("ISIS Slow")

    veto_all_bool_vals.append(veto_Wrong_Pulse)
    veto_all_naming.append("Wrong Pulse")

    veto_all_bool_vals.append(veto_TS2_Pulse)
    veto_all_naming.append("TS2 Pulse")

    veto_all_bool_vals.append(veto_SMP)
    veto_all_naming.append("SMP")

    veto_all_bool_vals.append(veto_FIFO)
    veto_all_naming.append("FIFO")

    for veto_num in veto_fast_chopper:
        veto_all_bool_vals.append(veto_fast_chopper[veto_num])
        veto_all_naming.append("Fast Chopper - " + veto_num)

    for veto_num in veto_External:
        veto_all_bool_vals.append(veto_External[veto_num])
        veto_all_naming.append("External Veto - " + veto_num)

    isVeto = True in veto_all_bool_vals

    return FrameNumber, EventsInFrame, FrameTime, PeriodNumber, PeriodSequence, isVeto, veto_all_bool_vals, veto_all_naming, binary_header

def PacketProcessor_MAPS(Packet_Data, SRC_IP):  # Pulls the event data out of the streamed packets
    # Note this function only gets MAPs Data from the event packet - Time from TOF, Position and pulse height

    numprocessedevents = 0
    numerror = 0

    Frame_Posisitions = []
    Frame_PulHs = []
    Frame_PosOverFlows =[]
    Frame_PulHOverFlows =[]
    Frame_Times = []
    df_WiringTable = WiringTableReader('DAES_WiringTable_TEST.csv')
    IPWiringTable = df_WiringTable.loc[(df_WiringTable['StreamingIP'] == SRC_IP)]
    CH_MantidDectID = IPWiringTable['Mantid_DetectorID_Start'].tolist()
    CH_MantidDectLen = IPWiringTable['Mantid_Detector_ID_Lenght'].tolist()

    numEvents = int((len(Packet_Data) / 16)-1) # work out the number of events in the packet by dividing 16 (event lenght) - and subtracting one for loop
    for event in range(0, numEvents):       # For each event in the packet
        EventStartADD = (event * 16)        # calculate the start address of the event data
        EventEndADD = EventStartADD + 16    # calculate the end address of the event data

        # hexEvent =    # Get the hex data from packet between the start/end of event
        binEvent = bin(int(Packet_Data[EventStartADD:EventEndADD], base=16))[2:]          # convert hex into binary - remove first two chars as they are always 0b
        if len(binEvent) == 64:   # if packet starts with e0 and has correct len

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
            print("INVALID EVENT DETECTED, Bad Event: ", Packet_Data[EventStartADD:EventEndADD], " Error SRC IP: ", SRC_IP)

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
