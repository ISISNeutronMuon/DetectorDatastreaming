# ISIS Electronics Support Group
# Core Python Scripts
#
# Used to work with IESG hardware within the python environment
# JJD 05/11/2021

# Main Classes:
# UDP Functions - core UDP class
# PC3544 - Merlin ADC class
# - GPS Slot 0 Commands
# KafkaFunctions - functions for sending and pulling data from Kafka Topics
# ESSFlatbuffers - functions for serialising and deserialising flatbuffer messages

# Import Required components
import os
import socket  # include socket function for network traffic
import time
import matplotlib.pyplot as plt
from confluent_kafka import Producer, Consumer, TopicPartition
import uuid
import json
import pandas as pd  # include pandas for CSV reading and any array functions
import datetime  # include date time to get date time values as needed.
from dateutil.tz import tzutc
from influxdb import InfluxDBClient  # used to log telemetry to the IESG monitoring server
import flatbuffers
import streaming_data_types.fbschemas.eventdata_ev42.EventMessage as EventMessage
import streaming_data_types.fbschemas.eventdata_ev42.FacilityData as FacilityData
import streaming_data_types.fbschemas.isis_event_info_is84.ISISData as ISISData
import numpy as np

# Define global variables for key info - possibly link from caller?
HostIP = "192.168.1.125"
write_register_ports = {"device": 10002, "host": 10003}
read_command_ports = {"device": 10000, "host": 10001}
receive_ports = {"device": 10000, "host": 10000}

# Kafka Variables
Kafka_broker = ""  # Broker for Kafka to use
kafka_event_topic = ""  # Kafka Topic for Event Data
kafka_runinfo_topic = ""  # Kafka Topic for run start/stop and control messages

# Instrument Setup Variables
PC3544_SwitchLookup = ""  # file to lookup Switch addresses to IP address and port
Instrument_Boards = ""  # file listing all of the boards within the specified system
Instrument_Wiring_Table = ""  # file location for the streaming wiring table


# Define a Class for all useful UDP functions the group might use
class UDPFunctions:
    # define initialisation commands
    def __init__(self, ip_address, host_ip=HostIP):
        self.IPAddress_Device = ip_address  # set IP to talk to
        self.IPAddress_Host = host_ip  # set the IP to send the traffic from
        # define network sockets
        self.UDPSocket = None  # generic socket for any use
        self.UDPSocket_write = None  # define the UDP socket object - writing data
        self.UDPSocket_receive = None  # define the UDP socket object - receiving data
        self.UDPSocket_read_command = None  # define the UDP socket object - sending read data command
        # give object port definitions
        self.ports_write = write_register_ports
        self.ports_read_command = read_command_ports
        self.ports_receive = receive_ports

    # define function to print out socket info
    def info(self):
        print("UDP Object Information: ")
        print("Host Info, IP: ", self.IPAddress_Host, ", Port: ", self.Port_Host)
        print("Device Info, IP: ", self.IPAddress_Device, ", Port: ", self.Port_Device)

    # function to open the UDP port on the computer
    def open(self, port_type="write", force_host_port=None):
        if not force_host_port == None:
            self.UDPSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.UDPSocket.bind((self.IPAddress_Host, force_host_port))
        elif port_type == "write":
            self.UDPSocket_write = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.UDPSocket_write.bind((self.IPAddress_Host, self.ports_write["host"]))
        elif port_type == "receive":
            self.UDPSocket_receive = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.UDPSocket_receive.bind((self.IPAddress_Host, self.ports_receive["host"]))
        elif port_type == "read_command":
            self.UDPSocket_read_command = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.UDPSocket_read_command.bind((self.IPAddress_Host, self.ports_read_command["host"]))
        else:
            Error.AddError(ErrorNumber=17,
                           ErrorDesc="Value Range Error - port type to open given.  "
                                     "unable to open a port within a port number to open",
                           Severity="Error", printToUser=True)

    # function to close the UDP port
    def close(self, port_type="write", force_host_port=None):
        if None != force_host_port:
            self.UDPSocket.close()
        elif port_type == "write":
            self.UDPSocket_write.close()
        elif port_type == "receive":
            self.UDPSocket_receive.close()
        elif port_type == "read_command":
            self.UDPSocket_read_command.close()
        else:
            Error.AddError(ErrorNumber=17,
                           ErrorDesc="Value Range Error - port type to open given.  "
                                     "unable to open a port within a port number to open",
                           Severity="Error", printToUser=True)

    # function to set the timeout time for the UDP socket
    # timeout = None - no timeout set
    # timeout = float - seconds until timeout is reached
    def set_timeout(self, timeout):
        self.UDPSocket.settimeout(timeout)

    # Writes a given value to a given register address - constructs message and writes to
    # Register address in hex to write to -
    # value to write - hex value to write -
    def register_write(self, register_address, value_to_write, delay=0):
        message = b""  # define the byte array to hold the message to send over UDP

        if register_address[:2] == "0x":  # if register address has the 0x hex identifier
            register_address = register_address[2:]  # remove it

        if value_to_write[:2] == "0x":  # if values to write has the 0x hex identifier
            value_to_write = value_to_write[2:]  # remove it

        register_address_len = len(register_address)  # Get length of register address

        # if the given address isn't in the correct output byte format - add error and try to fix
        if register_address_len % 8 != 0:
            Error.AddError(ErrorNumber=18,
                           ErrorDesc="Address Error - Incorrect register address length "
                                     "- added leading 0's in an attempt to resolve",
                           Severity="NOTICE", printToUser=True)
            # attempt to fix the error
            for i in range(8 - register_address_len):  # for amount of leading 0's to add
                register_address = "0" + register_address  # add leading zero
                register_address_len = len(register_address)  # correct register length

        # Add the register address to the message to send
        for i in range(int(register_address_len / 2)):
            char_start = i * 2
            message += bytes.fromhex(register_address[char_start: char_start + 2])

        value_to_write_len = len(value_to_write)  # get the length of the users data
        block_size = int(value_to_write_len / 8) + (value_to_write_len % 8 > 0)  # calc block size add 1 if remainder

        message += block_size.to_bytes(2, byteorder='big')  # add the blocksize to the UDP message

        # for each block of data
        for block in range(block_size):
            char_start = block * 8
            current_block = value_to_write[char_start: char_start + 8]
            if len(current_block) % 8 != 0:
                Error.AddError(ErrorNumber=17,
                               ErrorDesc="Value Range Error - Incorrect data length given to register_write "
                                         "- added leading 0's to attempt to resolve, which is likely to cause "
                                         "incorrect numbers being written to the PCB",
                               Severity="ERROR")
                # attempt to fix the error
                for i in range(8 - len(current_block)):  # for amount of leading 0's to add
                    current_block = "0" + current_block  # add leading zero

            # Add the block to the message to send
            for i in range(int(len(current_block) / 2)):
                char_start = i * 2
                message += bytes.fromhex(current_block[char_start: char_start + 2])
        self.open(port_type="write")
        self.UDPSocket_write.sendto(message, (self.IPAddress_Device, self.ports_write["device"]))
        time.sleep(delay)
        self.close(port_type="write")

    def register_read(self, register_address, block_size):
        message = b""  # define the byte array to hold the read command message

        if register_address[:2] == "0x":  # if register address has the 0x hex identifier
            register_address = register_address[2:]  # remove it
        register_address_len = len(register_address)  # Get length of register address

        # if the given address isn't in the correct output byte format - add error and try to fix
        if register_address_len % 8 != 0:
            Error.AddError(ErrorNumber=18,
                           ErrorDesc="Address Error - Incorrect register address length "
                                     "- added leading 0's in an attempt to resolve",
                           Severity="NOTICE")
            # attempt to fix the error
            for i in range(8 - register_address_len):  # for amount of leading 0's to add
                register_address = "0" + register_address  # add leading zero
                register_address_len = len(register_address)  # correct register length

        # Add the register address to the read command message to send
        for i in range(int(register_address_len / 2)):
            char_start = i * 2
            message += bytes.fromhex(register_address[char_start: char_start + 2])

        if not isinstance(block_size, int):
            Error.AddError(ErrorNumber=15, Severity="ERROR", printToUser=True,
                           ErrorDesc="Value Type Error - block size of UDP Register read not an int, "
                                     "expected as a int for the number of blocks to read back from the board")
            return "ERROR"
        else:
            message += block_size.to_bytes(2, byteorder='big')  # add the blocksize to the UDP message

        # complete UDP operations
        UDPFunctions.open(self, port_type="receive")  # open ports
        UDPFunctions.open(self, port_type="read_command")
        self.UDPSocket_read_command.sendto(message, (
            self.IPAddress_Device, self.ports_read_command["device"]))  # send read command
        data, address = self.UDPSocket_receive.recvfrom(1024)  # receive byte array with the data
        UDPFunctions.close(self, port_type="receive")  # close ports
        UDPFunctions.close(self, port_type="read_command")
        returned = data.hex()  # convert byte array into hex string
        returned = "0x" + returned[12:]  # remove read command from data
        return returned  # return the read register value

    # Writes a given value to  given register address and then checks if it was written correctly
    def register_write_verify(self, register_address, value_to_write, delay=0):
        value_to_write_len = len(value_to_write)
        block_size = int((value_to_write_len - 2) / 8) + ((value_to_write_len - 2) % 8 > 0)  # calc block size

        UDPFunctions.register_write(self, register_address, value_to_write, delay=delay)  # write the data
        time.sleep(delay)
        read_value = UDPFunctions.register_read(self, register_address=register_address,
                                                block_size=block_size)  # read register
        time.sleep(delay)

        # add padding (if required) to value to write to match read val
        expected_read_len = (block_size * 8) + 2
        if value_to_write_len % expected_read_len != 0:
            Error.AddError(ErrorNumber=15,
                           ErrorDesc="Value type Error - Incorrect data length given to register_write_verify "
                                     "adding leading zeros to attempt to resolve the issue",
                           Severity="ERROR")
            # attempt to fix the error
            for i in range(expected_read_len - value_to_write_len):  # for amount of leading 0's to add
                value_to_write = value_to_write[:2] + "0" + value_to_write[2:]  # add leading zero

        if read_value == '0xdeadbeef':
            Error.AddError(ErrorNumber=20, printToUser=True,
                           ErrorDesc="Register Read Error - read back ""0xdeadbeef"" from the register read, "
                                     "this is generally caused by a flash fault",
                           Severity="WARNING")
        return value_to_write == read_value


# Define Merlin ADC Processor Board Class - all functions for PC3544m4
class PC3544:
    # on MADC object creation
    def __init__(self, switchposition):
        if switchposition in range(32):  # Validate inputted switch position - within 0-31
            self.switch_pos = int(switchposition)  # If valid get objects switch pos
        else:  # If incorrect add error to error list
            Error.AddError(17, "Value Range Error - Switch Position is out of range, set to 0")
            self.switch_pos = 0  # Default to pos 0

        self.MADC_IPs = self.get_network_ip()  # Get IP info from the Switch Position
        self.MADC_Ports = self.get_network_port()  # Get port info from the Switch Position

        self.control_ipaddress = self.MADC_IPs["BE_FPGA_IP"]  # Get BE/Control IP from dict
        self.control_port_W = self.MADC_Ports["BE_FPGA_PORT_W"]  # Get BE/Control port from dict
        self.control_port_R = self.MADC_Ports["BE_FPGA_PORT_R"]  # Get BE/Control port from dict
        self.AddressMap = self.get_reg_address_map()  # Get the Address Map Information
        self.control_socket = self.setup_control_network()  # setup control socket

    # Function to get the MADC's 5 IP addresses from its switch position
    def get_network_ip(self):
        Possible_BE_IP = [148, 149, 150, 155, 156, 157, 158, 159, 160, 165, 166, 167, 168, 169, 170, 175,
                          176, 177, 178, 179, 180, 185, 186, 187, 188, 189, 190, 195, 196, 197, 198, 199, 200]
        BE_FPGA_IP = "192.168.1." + str(Possible_BE_IP[self.switch_pos])  # BE IP = 192.168.1. Matching end number
        FE_FPGA0_IP = "192.168.2." + str(100 + (self.switch_pos * 4))  # Calc FE FPGA0 IP
        FE_FPGA1_IP = "192.168.2." + str(101 + (self.switch_pos * 4))  # Calc FE FPGA1 IP
        FE_FPGA2_IP = "192.168.2." + str(102 + (self.switch_pos * 4))  # Calc FE FPGA2 IP
        FE_FPGA3_IP = "192.168.2." + str(103 + (self.switch_pos * 4))  # Calc FE FPGA3 IP
        return {"BE_FPGA_IP": BE_FPGA_IP, "FE_FPGA0_IP": FE_FPGA0_IP, "FE_FPGA1_IP": FE_FPGA1_IP,
                "FE_FPGA2_IP": FE_FPGA2_IP, "FE_FPGA3_IP": FE_FPGA3_IP}  # return the Addresses as a dictionary

    # Get the network ports the MADC uses from the switch position
    def get_network_port(self):
        BE_FPGA_PORT_R = 0  # Set BE read as port 0 - currently not used
        BE_FPGA_PORT_W = 10002  # Set BE write as port 0 - currently not used
        FE_FPGA0_PORT = 48640 + (self.switch_pos * 4)  # Calc FE FPGA0 port number
        FE_FPGA1_PORT = 48641 + (self.switch_pos * 4)  # Calc FE FPGA1 port number
        FE_FPGA2_PORT = 48642 + (self.switch_pos * 4)  # Calc FE FPGA2 port number
        FE_FPGA3_PORT = 48643 + (self.switch_pos * 4)  # Calc FE FPGA3 port number
        return {"BE_FPGA_PORT_R": BE_FPGA_PORT_R, "BE_FPGA_PORT_W": BE_FPGA_PORT_W, "FE_FPGA0_PORT": FE_FPGA0_PORT,
                "FE_FPGA1_PORT": FE_FPGA1_PORT,
                "FE_FPGA2_PORT": FE_FPGA2_PORT, "FE_FPGA3_PORT": FE_FPGA3_PORT}  # Return ports as a dictionary

    # Configure a network socket for the control connection to the MADC
    def setup_control_network(self):
        control_network = UDPFunctions(self.control_ipaddress)
        return control_network  # return the socket

    # Gets the register addresses for PC3544 from the AddressMap file
    def get_reg_address_map(self, address_map_location=".\IESG_AddressMap.csv"):
        address_map = pd.read_csv(address_map_location)
        PC3544_add_map = address_map[address_map['PC Number'] == "PC3544"]
        return PC3544_add_map

    # Sets a single channel gain on the MADC board.
    # channel = #x where # is the channel number, and x is A or B
    # gain, value to write - converted to hex if no 0x leader
    # returns true if sucessful, false on failure
    def set_gain(self, input_channel, gain, WriteType="Verify", write_FPGA=True, write_flash=False):
        success_list = []  # list to hold if each register write type completed correctly
        if not isinstance(input_channel, str):
            Error.AddError(ErrorNumber=15, Severity="ERROR", printToUser=True,
                           ErrorDesc="Value Type Error - set gain expected channel number to "
                                     "be a str to account for A/B's. - Gain not set")
            return False
        else:
            channel_ab = input_channel[-1]  # A/B is going to be the last char from the channel string
            channel_no = int(input_channel[:-1])  # everything else will be the channel number

        if isinstance(gain, str):  # if set gain is in string format
            if not gain[:2] == "0x":  # if data is not hex (assume int)
                gain = hex(int(gain))  # convert to hex and add 0x leader
                Error.AddError(ErrorNumber=15, Severity="WARNING",
                               ErrorDesc="Value Type Error - if data type is string to set gain, "
                                         "expected hex (with 0x hex leader) converted int into a hex value")
        elif isinstance(gain, int):  # if gain input is an integer value
            gain = hex(gain)  # convert int into a hex string

        # code to pad out the gain value to full 8 byte word
        LeadingZeros = ""
        for i in range(10 - len(gain)):  # for amount of leading 0's to add - (0x + 8 bytes) - current lenght
            LeadingZeros += "0"  # add leading zero
        gain = gain[:2] + LeadingZeros + gain[2:]

        FE_FPGA_NO = int(channel_no / 6)  # get FPGA number by dividing the channel by 6 - as ints
        FE_FPGA_CH_No = channel_no - (FE_FPGA_NO * 6)  # get the ADC channel number on the FPGA

        # filter the address map for the register address to use
        gain_address_map = self.AddressMap[self.AddressMap['Register Function'] == "GAIN"]
        if write_FPGA:
            channel_add_map = gain_address_map[
                gain_address_map["Register Name"] == "FE_FPGA-CH" + str(FE_FPGA_CH_No) + channel_ab]
            Register = channel_add_map.iloc[0]["Instance " + str(FE_FPGA_NO)]

            if WriteType == "Verify":
                Verify_Status = self.control_socket.register_write_verify(register_address=Register,
                                                                          value_to_write=gain)
                if not Verify_Status:
                    Error.AddError(ErrorNumber=13, Severity="ERROR", printToUser=True,
                                   ErrorDesc="UDP Verify Error - register verification failed, "
                                             "incorrect value within register")
                success_list.append(Verify_Status)
            elif WriteType == "Write":
                self.control_socket.register_write(register_address=Register, value_to_write=gain)
                success_list.append(True)
            else:
                Error.AddError(ErrorNumber=19, Severity="ERROR", printToUser=True,
                               ErrorDesc="Command Syntax Error - Unknown register write type given, "
                                         "expected Verify or Write - no values writen")
                success_list.append(False)
        if write_flash:
            channel_add_map = gain_address_map[
                gain_address_map["Register Name"] == "FE_FLASH-CH" + input_channel]
            register_addresses = [channel_add_map.iloc[0]["Instance " + str(i)] for i in range(4)]
            GainBlocks = [("0x000000" + gain[(i * 2) + 2:(i * 2) + 4]) for i in range(4)]
            if WriteType == "Verify":
                Verify_Status = [self.control_socket.register_write_verify(register_address=register_addresses[i],
                                                                           value_to_write=GainBlocks[i], delay=0.1)
                                 for i in range(4)]
                print("Gain verify stat: " + str(Verify_Status))
                success_list.append(Verify_Status)

                if not all(Verify_Status):
                    Error.AddError(ErrorNumber=13, Severity="ERROR", printToUser=True,
                                   ErrorDesc="UDP Verify Error - register verification failed, "
                                             "incorrect value within register")
            elif WriteType == "Write":
                Verify_Status = [self.control_socket.register_write(register_address=register_addresses[i],
                                                                    value_to_write=GainBlocks[i], delay=0.1)
                                 for i in range(4)]
                success_list.append(True)
            else:
                Error.AddError(ErrorNumber=19, Severity="ERROR", printToUser=True,
                               ErrorDesc="Command Syntax Error - Unknown register write type given, "
                                         "expected Verify or Write - no values writen")
        return all(success_list)  # return true if everything is successful

    def set_gain_list(self):
        pass

    def set_dsp(self):
        pass

    def start_streams(self):
        pass

    def stop_streams(self):
        pass

    def get_monitoring(self):
        pass

    def get_serial(self):
        pass

    def get_info_brdtemp(self):
        pass

    def get_info_brdpowerinfo(self):
        pass

    def get_board_info(self):
        pass

    def set_dest_ip(self):
        pass

    def set_dest_mac(self):
        pass

    def set_dest_fpga_ports(self):
        pass


# Class for DAE Streaming Code. Handles the receiving and processing of UDP data to Kafka.
class dae_streamer:
    def __init__(self, switchposition=None, fe_fpga=None, stream_ip=None, stream_port=None,
                 setup_wtable_onstartup=True, instrument=None, kafka_broker=None, influxdb_database=None):
        # try getting the streaming information from the Stream ip and port information
        if stream_ip is not None and stream_port is not None:
            self.ip = stream_ip
            self.port = stream_port
        # if not directly specified use the SW pos and FE_FPGA numbers
        elif switchposition is not None and fe_fpga is not None:
            if switchposition in range(32):  # Validate inputted switch position - within 0-31
                self.switch_pos = int(switchposition)  # If valid get objects switch pos
            else:  # If incorrect add error to error list
                Error.AddError(17, "Value Range Error - Switch Position is out of range, set to 0")
                self.switch_pos = 0  # Default to pos 0

            fe_fpga_ip_offset = [100, 101, 102, 103]
            fe_fpga_port_offset = [48640, 48641, 48642, 48643]
            self.ip = "192.168.2." + str(fe_fpga_ip_offset[fe_fpga] + (self.switch_pos * 4))  # Calc stream IP
            self.port = (fe_fpga_port_offset[fe_fpga] + (self.switch_pos * 4))  # Calc stream Port

        self.stream_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # create a network socket
        self.setup_network()

        # setup variables for ADC wiring configuration
        self.CH_MantidDectID = None
        self.CH_MantidDectLen = None
        if setup_wtable_onstartup is True:
            self.setup_wiring_map()

        # setup Kafka object for sending data
        if instrument is not None:
            if kafka_broker is not None:
                self.kafka_object = kafka_helper(kafka_broker=kafka_broker, instrument=instrument)
            else:
                self.kafka_object = kafka_helper(kafka_broker="hinata.isis.cclrc.ac.uk:9092", instrument=instrument)
                Error.AddError(ErrorNumber=22, Severity="NOTICE", printToUser=True,
                               ErrorDesc="Function Fallback Error - kafka broker address not specified. "
                                         "Using default broker of: hinata.isis.cclrc.ac.uk:9092")
        else:
            Error.AddError(ErrorNumber=22, Severity="NOTICE", printToUser=True,
                           ErrorDesc=(
                                       "Function Fallback Error, generation of DAE Streamer "
                                       "object has no instrument specified. Unable to setup kafka object"))
            self.kafka_object = None

        # setup influx logging
        self.influx_logger = InfluxDB_Wrapper(influxdb_database)
        self.thread_name = None

    # sets up network port to be used for streaming
    def setup_network(self):
        self.stream_socket.bind((HostIP, int(self.port)))
        self.stream_socket.settimeout(2)

    def setup_wiring_map(self, wiring_table='DAES_WiringTable_MAPs.csv'):
        df_wiring_table_full = pd.read_csv(wiring_table)
        df_wiring_table_ip = df_wiring_table_full.loc[(df_wiring_table_full['StreamingIP'] == self.ip)]
        self.CH_MantidDectID = df_wiring_table_ip['Mantid_DetectorID_Start'].tolist()
        self.CH_MantidDectLen = df_wiring_table_ip['Mantid_Detector_ID_Lenght'].tolist()

    def stream_loop_to_kafka(self, stop_threads, print_thread_lock, thread_name):
        self.thread_name = thread_name
        packet_count = 0
        while True:
            if stop_threads is True:
                print_thread_lock.aquire()
                print(thread_name, " - STOPPED")
                print_thread_lock.release()
                break
            try:
                if socket.gethostbyname(self.ip):
                    current_packet, source_address = self.stream_socket.recvfrom(900400)
                    packet_count += 1
                    self.process_packet_complete(current_packet.hex())
            except socket.timeout:
                if stop_threads():
                    break
                continue
        #return packet_count

    def process_packet_complete(self, packet_data):
        packet_frames = self.process_packet_frame_splitter(packet_data)
        mapped_headers = map(self.process_fheader, packet_frames)
        header_values = list(mapped_headers)
        total_events = 0
        for frame in range(len(packet_frames)):
            current_frame = packet_frames[frame]
            events = self.process_multiple_fevents_maps(current_frame)
            events_time = [event[1] for event in events]
            detector_ids = [event[0] for event in events]
            frame_ev42 = self.process_ev42(header_values[frame][0], header_values[frame][2], events_time, detector_ids)
            if self.kafka_object is not None:
                self.kafka_object.send_event_flatbuffer(frame_ev42)
            total_events += len(events_time)
        self.influx_logger.write_data()
        return total_events

    # splits a given packet into a list of frames
    @staticmethod
    def process_packet_frame_splitter(packet):
        header_str = "ffffffffffffffff0"
        header_str2 = "fffffffffcffffff0"
        end_marker_str = "efffffffffffffff0"
        packing_str = "0000000000000000"

        replacement_header_str = ":ffffffffffffffff0"
        replacement_header_str2 = ":fffffffffcffffff0"
        replacement_end_marker_str = ""
        replacement_packing_str = ""

        processed_packet = packet.replace(header_str, replacement_header_str) \
            .replace(header_str2, replacement_header_str2) \
            .replace(end_marker_str, replacement_end_marker_str) \
            .replace(packing_str, replacement_packing_str)

        frames = processed_packet.split(":")
        frames.pop(0)  # remove first frame as the split occurs at frame start - always empty

        return frames

    # processes the header of a frame
    @staticmethod
    def process_fheader(frame_data):

        epoch = datetime.date(1970, 1, 1)

        frame_header = frame_data[0:128]  # extract header from frame data
        binary_header = bin(int(frame_header, base=16))[2:]  # convert to bin - remove bin identifier

        epoch = datetime.date(1970, 1, 1)
        current_date = datetime.date(int(binary_header[128:136], 2)+2000, 1, 1)
        delta_time = current_date - epoch
        days_since_epoch = delta_time.days + int(binary_header[136:145], 2) - 1

        # calculate time of frame event in nS - since EPOCH
<<<<<<< Updated upstream
        frame_time = int(((days_since_epoch * 8.64e+13) + (int(binary_header[145:150], 2) * 3.6e+12) +
=======
        frame_time = int((((365.25 * 8.64e+13) * int(binary_header[128:136], 2) + 30) +
                      (int(binary_header[136:145], 2) * 8.64e+13) + (int(binary_header[145:150], 2) * 3.6e+12) +
>>>>>>> Stashed changes
                      (int(binary_header[150:156], 2) * 6e+10) + (int(binary_header[156:162], 2) * 1e+9) +
                      (int(binary_header[162:172], 2) * 1000000) + (int(binary_header[172:182], 2) * 1000) +
                      int(binary_header[182:192], 2)))

        current_date = datetime.date(int(binary_header[128:136], 2), 1, 1) + int(binary_header[136:145], 2)
        days_since_epoch = current_date-epoch

        print(days_since_epoch.days)

        period_number = int(binary_header[208:224], 2)
        period_sequence = int(binary_header[192:207], 2)
        f_number = int(binary_header[96:128], 2)
        f_number_events = int(binary_header[224:256], 2)
        ppp = int(binary_header[256:288], 2)

        vetos = {}  # dict holding all veto info

        veto_frame_data_overrun = False
        veto_frame_mem_full = False
        veto_no_frame_sync = False
        veto_bad_frame = False

        vetos["veto_isis_slow"] = binary_header[315:316] == "1"
        vetos["veto_wrong_pulse"] = binary_header[316:317] == "1"
        vetos["veto_ts2_pulse"] = binary_header[317:318] == "1"
        vetos["veto_smp"] = binary_header[318:319] == "1"
        vetos["veto_fifo"] = binary_header[319:320] == "1"

        veto_fast_chopper_str = binary_header[307:311]
        veto_external_str = binary_header[311:315]
        for i in range(len(veto_fast_chopper_str)):
            vetos[("Fast Chopper - " + str(i))] = veto_fast_chopper_str[i] == "1"
        for char in range(len(veto_external_str)):
            vetos[("External - " + str(i))] = veto_external_str[char] == "1"

        is_veto = True in vetos

        return f_number, f_number_events, frame_time, period_number, period_sequence, ppp, is_veto, vetos

    # processes the event data within a frame
    def process_multiple_fevents_maps(self, data, frame_event_number=None):
        event_data = data[128:]
        event_data_len = len(event_data) - event_data.count('\n')

        if event_data_len % 16 != 0:  # if event data is complete - each event is 16 char, -1 for ending \n char
            Error.AddError(ErrorNumber=22, Severity="ERROR", printToUser=False,
                           ErrorDesc="Event Data Error - event data not divisible by 16, suspected incomplete"
                                     " or other frame issue. event data len: " + str(event_data_len))
            return None

        num_events = int(event_data_len / 16)
        if frame_event_number is not None and frame_event_number == num_events:
            Error.AddError(ErrorNumber=23, Severity="ERROR", printToUser=True,
                           ErrorDesc="Event Number mismatch - number of frames from header and event size do not match")
            return None
        processed_events = []
        for event_i in range(num_events):
            event_start = event_i * 16
            event_end = event_start + 16
            processed_events.append(self.process_single_fevent_maps(event_data[event_start:event_end]))
        return processed_events

    # processes a single event, returns mantid detector information etc
    def process_single_fevent_maps(self, event_data):
        bin_event = bin(int(event_data, base=16))[2:]  # convert hex into binary - remove two char as always 0b
        if event_data[:2] == "e0":  # if has correct marker
            event_pulse_height_overflow = bin_event[38:39]
            event_position_overflow = bin_event[39:40]

            event_time_to_frame = int(bin_event[8:32], 2)  # convert binary frame time of event into an int
            event_position = int(bin_event[52:64], 2)  # convert binary position of the event into an int
            event_pulse_height = int(bin_event[40:52], 2)  # convert binary pulse height of the event into an int
            adc_channel = int(bin_event[36:38], 2)  # convert binary channel of the event into an int

            mantid_pixel = int(event_position / (4096 / self.CH_MantidDectLen[adc_channel])) \
                           + self.CH_MantidDectID[adc_channel]  # Move to mantid tube location

            self.influx_logger.add_event_to_json(self.thread_name, self.ip, self.port, pulse_height=event_pulse_height,
                                                 mantid_pixel=mantid_pixel, tube_position=event_position)

            return event_time_to_frame, mantid_pixel, event_position, event_pulse_height, event_position_overflow, \
                event_pulse_height_overflow
        else:
            Error.AddError(ErrorNumber=25, Severity="NOTICE", printToUser=True,
                           ErrorDesc=(
                                       "Event Data Error - Missing e0 flag in event - SKIPPING: " + event_data))
            return None

    # processes into ev42 schema
    def process_ev42(self, frame_number, frame_time, event_time, detector_id):
        """
        Serialise event data as an ev42 FlatBuffers message.
        :param self:           -source (ip)
        :param frame_number:      -message ID
        :param frame_time: -pulse time
        :param event_time: - list of time of flight
        :param detector_id:     mantid_detector_id - list
        # :param isis_specific:
        :return:
        """
        builder = flatbuffers.Builder(1024)
        builder.ForceDefaults(True)

        source = builder.CreateString(self.ip)

        tof_data = builder.CreateNumpyVector(np.asarray(event_time).astype(np.uint32))
        det_data = builder.CreateNumpyVector(np.asarray(detector_id).astype(np.uint32))

        # isis_data = None
        # if isis_specific:
        # # isis_builder = flatbuffers.Builder(96)
        #     ISISData.ISISDataStart(builder)
        # ISISData.ISISDataAddPeriodNumber(builder, isis_specific["period_number"])
        # ISISData.ISISDataAddRunState(builder, isis_specific["run_state"])
        # ISISData.ISISDataAddProtonCharge(builder, isis_specific["proton_charge"])
        # isis_data = ISISData.ISISDataEnd(builder)

        # Build the actual buffer
        EventMessage.EventMessageStart(builder)
        EventMessage.EventMessageAddDetectorId(builder, det_data)
        EventMessage.EventMessageAddTimeOfFlight(builder, tof_data)
        EventMessage.EventMessageAddPulseTime(builder, frame_time)
        EventMessage.EventMessageAddMessageId(builder, frame_number)
        EventMessage.EventMessageAddSourceName(builder, source)

        # if isis_specific:
        #     EventMessage.EventMessageAddFacilitySpecificDataType(
        # builder, FacilityData.FacilityData.ISISData
        #     EventMessage.EventMessageAddFacilitySpecificData(builder, isis_data)

        data = EventMessage.EventMessageEnd(builder)

        builder.Finish(data, file_identifier=b"ev42")
        return bytes(builder.Output())

    # gets latest network packet
    def network_get_last_packet(self):
        try:
            if socket.gethostbyname(self.ip):
                packet, source_ip = self.stream_socket.recvfrom(900400)
        except self.stream_socket.timeout:
            # no new packet on port
            packet = None
            source_ip = None
        return packet, source_ip

    # reads in data for testing, from a .txt file. if line to read is -1, returns list of example packets
    def test_read_file_as_packets(self, filename, line_to_read, src_ip=None, file_path=None):
        # if no source IP is given assume its the objects IP address
        if src_ip is None:
            src_ip = self.ip

        if file_path is None:
            test_file = open(filename, "r")
        else:
            file = os.path.join(file_path, filename + ".txt")
            test_file = open(file, "r")

            packet_list = []
            source_ip_list = []
            for line in test_file:
                packet_list.append(line)
                source_ip_list.append(src_ip)

        if line_to_read == -1:
            packet = packet_list
            source_ip = source_ip_list
        else:
            packet = packet_list[line_to_read]
            source_ip = source_ip_list[line_to_read]

        test_file.close()
        return packet, source_ip


# Error Handler class - define once on program start to be able to log all errors within the program
class ErrorHandler:
    def __init__(self):
        self.ErrorNumberList = []
        self.ErrorDescList = []
        self.ErrorSeverity = []

    # Checks to see if the current Error list is valid - returns true if valid
    def CheckErrors_Valid(self):
        if len(self.ErrorNumberList) != len(self.ErrorDescList):
            print("Error List Length Mismatch")
            return False
        if len(self.ErrorSeverity) != len(self.ErrorNumberList):
            print("Error List Length Mismatch")
            return False
        return True

    def print_error(self, error_num):
        if self.ErrorSeverity[error_num] == "ERROR":
            print("\033[31m\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num],
                  "- (", self.ErrorNumberList[error_num], ")", self.ErrorDescList[error_num], "\033[0m")
        elif self.ErrorSeverity[error_num] == "WARNING":
            print("\033[33m\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num],
                  "- (", self.ErrorNumberList[error_num], ")", self.ErrorDescList[error_num], "\033[0m")
        elif self.ErrorSeverity[error_num] == "NOTICE":
            print("\033[34m\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num],
                  "- (", self.ErrorNumberList[error_num], ")", self.ErrorDescList[error_num], "\033[0m")
        else:
            print("\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num], "- (", self.ErrorNumberList[error_num],
                  ")", self.ErrorDescList[error_num], "\033[0m")
        return True

    def AddError(self, ErrorNumber=0, ErrorDesc="unknown error has occured (default)",
                 Severity="ERROR", printToUser=False):

        self.ErrorNumberList.append(ErrorNumber)
        self.ErrorDescList.append(ErrorDesc)
        self.ErrorSeverity.append(Severity)
        self.CheckErrors_Valid()
        if printToUser:
            self.print_error(len(self.ErrorNumberList) - 1)

    def ClearErrors(self):
        self.ErrorNumberList = []
        self.ErrorDescList = []
        self.ErrorSeverity = []

    # Function to print all errors to terminal, returns false if an errors are invalid, true if printed
    def print_all(self):
        if not self.CheckErrors_Valid():
            return False
        if len(self.ErrorNumberList) == 0:
            print("\033[92mProgram currently has 0 errors")
            return True
        else:
            print("Program currently has ", len(self.ErrorNumberList), " errors")
        for error_number in range(len(self.ErrorNumberList)):
            print("Error ", error_number, ": ", end='')
            self.print_error(error_number)
        return True

    # Function to print last errors to terminal, returns false if an errors are invalid, true if printed
    def print_last(self):
        numErrors = len(self.ErrorNumberList)
        if not self.CheckErrors_Valid():
            return False
        if numErrors == 0:
            print("Program currently has 0 errors")
            return True
        else:
            print("Program currently has", numErrors, "errors")
            print("The last error was: ", self.ErrorNumberList[numErrors], " - ", self.ErrorDescList[numErrors])


# Wrapper to Handle Influx Monitoring within the python environment
class InfluxDB_Wrapper:
    def __init__(self, database, time_precision="ms", host="NDAIESGMonitor.isis.cclrc.ac.uk", port=8086):
        self.influx_client = InfluxDBClient(host, port)
        self.database = database  # database for the wrapper to use
        self.t_precision = time_precision  # precision of the data
        self.json_data = []  # store of data to write to influx

    def write_data(self):

        self.influx_client.write_points(self.json_data, database=self.database, time_precision=self.t_precision)
        self.json_data = []
         #   except exceptions InfluxDBClientError as err:

    def add_json(self, json_to_add):
        self.json_data.append(json_to_add)
        return True

    def add_event_to_json(self, thread_instance, stream_address, stream_port, pulse_height=None, mantid_pixel=None,
                          tube_position=None):
        to_log = {}
        if pulse_height is not None:
            to_log["Pulse Height"] = pulse_height
        if mantid_pixel is not None:
            to_log["Mantid Pixel"] = mantid_pixel
        if tube_position is not None:
            to_log["Tube Position"] = tube_position

        if len(to_log) != 0:
            self.json_data.append(
                {
                    "measurement": "Python_Streamer",
                    "tags": {
                        "thread": thread_instance,
                        "MADC_IP_Address": stream_address,
                        "MADC_Port": stream_port
                    },
                    "time": str(datetime.datetime.utcnow()),
                    "fields": to_log
                }
            )
        else:
            Error.AddError(ErrorNumber=22, Severity="ERROR", printToUser=False,
                           ErrorDesc="Function Fallback Error - No value was passed to influx logger to log.")



    def add_frame_to_json(self, event_count, vetos, ):
        pass

    def add_packet_to_json(self):
        pass


# code to deal with writing and reading data from a given Kafka broker
class kafka_helper():
    def __init__(self, kafka_broker, instrument, event_topic="_events", run_info_topic="_runInfo",
                 monitors_topic="_monitorHistograms", sample_env_topic="_sampleEnv"):

        self.server = kafka_broker
        self.producer = Producer({"bootstrap.servers": self.server})
        # build string for each topic name
        self.topic_event = instrument + event_topic
        self.topic_run_info = instrument + run_info_topic
        self.topic_monitors = instrument + monitors_topic
        self.topic_sample_environment = instrument + sample_env_topic

    def send_event_flatbuffer(self, ev42_data):
        self.producer.produce(self.topic_event, ev42_data)
        self.producer.flush()

    def send_start_stop_flatbuffer(self, run_control_message):
        self.producer.produce(self.topic_event, run_control_message)
        self.producer.flush()

    def get_event_data_range(self, start_time, end_time):
        """
        Get the data between the two given times.
        Note that this is based on the timestamp of when the data was put into kafka.
        Args:
            start_time: The beginning of where you want the data from
            end_time: The end of where you want the data to
            instrument: The instrument to get the data from
        """
        consumer = self._create_consumer()

        epoch = datetime.fromtimestamp(0, tzutc())

        start_time = start_time.astimezone(tzutc())
        end_time = end_time.astimezone(tzutc())

        def get_part_offset(dt):
            time_since_epoch = int((dt - epoch).total_seconds() * 1000)
            return consumer.offsets_for_times([TopicPartition(self.topic_event), 0, time_since_epoch])[0]
        try:
            start_time_part_offset = get_part_offset(start_time)
            end_time_part_offset = get_part_offset(end_time)
        except Exception:
            # Sometimes the consumer isn't quite ready, try once more
            start_time_part_offset = get_part_offset(start_time)
            end_time_part_offset = get_part_offset(end_time)

        offsets = [start_time_part_offset.offset, end_time_part_offset.offset]

        if offsets[0] == -1:
            print("No data found for time period")
            return

        consumer.assign([start_time_part_offset])

        if offsets[1] == -1:
            offsets[1] = consumer.get_watermark_offsets(end_time_part_offset)[1]

        number_to_consume = offsets[1] - offsets[0]

        return [json.loads(str(data.value(), encoding="utf-8"))
                for data in consumer.consume(number_to_consume)]

    def _create_consumer(self):
        consumer = Consumer(
            {
                "bootstrap.servers": self.server,
                "group.id": uuid.uuid4(),
            })
        return consumer


Error = ErrorHandler()  # create error handler object to hold all errors within


def test():
    stream_test = dae_streamer(16, 1)
    full_test_data = stream_test.test_read_file_as_packets('185_packets', -1,
                                                           file_path='C:\\GIT\\DetectorDatastreaming\\OOP Edition\\test_data')
    test_packets = full_test_data[0]  # remove IP address to get raw packet data
    full_test_packets = []
    for i in range(1):
        full_test_packets.extend(test_packets)

    print("Packets read in, packet count: ", len(full_test_packets))
    print("Perf counter start")
    timer_s = datetime.datetime.now()
    test_frames = []
    for i in range(len(full_test_packets)):
        test_frames.extend(stream_test.process_packet_frame_splitter(full_test_packets[i]))

    print("Packets splits into frames, total frames: ", len(test_frames))

    mapped_header = map(stream_test.process_fheader, test_frames)
    header_values = list(mapped_header)

    print("Headers processed, total headers: ", len(header_values))

    test_events_map = map(stream_test.process_multiple_fevents_maps, test_frames)
    test_events = list(test_events_map)
    test_all_events = []
    for event in test_events:
        if event is not None:
            test_all_events.extend(event)
    print("Events processed, total events: ", len(test_events))

    timer_e = datetime.datetime.now()
    delta = timer_e - timer_s
    timer = delta.total_seconds()
    print("Processing complete, Perf counter stop")

    rates = {"Packets": len(full_test_packets) / timer, "Frames": len(test_frames) / timer,
             "Events": len(test_all_events) / timer}

    sec_per = {"Packets": timer / len(full_test_packets), "Frames": timer / len(test_frames),
               "Events": timer / len(test_all_events)}

    totals = {"Packets": len(full_test_packets), "Frames": len(test_frames), "Events": len(test_all_events)}
    print("Task Completed in: ", timer, " Seconds")
    print("")
    print("File Processing metrics:")
    print("%8s: %14s %20s %24s" % ("Type:", "Total:", "Per Second:", "Time Per (Seconds)"))
    for value in totals:
        print("%8s: %14s %20s %24s " % (value, totals[value], rates[value], sec_per[value]))

    print("")
    print("Program Error Check: ")

    Error.print_all()


def test2():
    # do not specify instrument to not log to kafka
    stream_test = dae_streamer(switchposition=16, fe_fpga=1,
                               kafka_broker="livedata.isis.cclrc.ac.uk", influxdb_database="python_testing")
    full_test_data = stream_test.test_read_file_as_packets('184_packets', -1,
                                                           file_path='C:\\GIT\\DetectorDatastreaming_development\\OOP Edition\\test_data')
    test_packets = full_test_data[0]  # remove IP address to get raw packet data
    full_test_packets = []
    for i in range(1):
        full_test_packets.extend(test_packets)

    print("Packets read in, packet count: ", len(full_test_packets))
    print("Perf counter start")
    timer_s = datetime.datetime.now()
    event_count = 0
    for packet in full_test_packets:
        event_count += stream_test.process_packet_complete(packet)
    print("Events processed, total events: ", event_count)
    timer_e = datetime.datetime.now()
    delta = timer_e - timer_s
    timer = delta.total_seconds()
    print("Processing complete, Perf counter stop")
    print("Task Completed in: ", timer, " Seconds")
    print("rate: ", event_count / timer, " events/S")
    print("per event: ", timer / event_count, " S")
    print("")
    print("Program Error Check: ")

    Error.print_all()


if __name__ == "__main__":
    # with cProfile.Profile() as pr:
    # test()
    test2()
    Error.print_all()
