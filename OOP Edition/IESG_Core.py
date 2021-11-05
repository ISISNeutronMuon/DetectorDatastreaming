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
import socket           # include socket function for network traffic
import pandas as pd     # include pandas for CSV reading and any array functions
import datetime         # include date time to get date time values as needed.

# Define global variables for key info - possibly link from caller?

# Kafka Variables
Kafka_broker = ""               # Broker for Kafka to use
kafka_event_topic = ""          # Kafka Topic for Event Data
kafka_runinfo_topic = ""        # Kafka Topic for run start/stop and control messages

# Instrument Setup Variables
PC3544_SwitchLookup = ""        # file to lookup Switch addresses to IP address and port
Instrument_Boards = ""          # file listing all of the boards within the specified system
Instrument_Wiring_Table = ""    # file location for the streaming wiring table

# Define a Class for all useful UDP functions the group might use
class UDPFunctions:
    # define initialisation commands
    def __init__(self,host_ip, host_port, ip_address, port):
        self.IPAddress_Device = ip_address     # set IP to talk to
        self.Port_Device = port                # Set port to use
        self.IPAddress_Host = host_ip
        self.Port_Host = host_port
        self.UDPSocket = None

    # define function to print out socket info
    def info(self):
        print("UDP Object Information: ")
        print("Host Info, IP: ", self.IPAddress_Host, ", Port: ", self.Port_Host)
        print("Device Info, IP: ", self.IPAddress_Device, ", Port: ", self.Port_Device)

    # function to open the UDP port on the computer
    def open(self):
        self.UDPSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.UDPSocket.bind(self.IPAddress_Host, self.Port_Host)

    # function to close the UDP port
    def close(self):
        self.UDPSocket.close()

    # function to set the timeout time for the UDP socket
    # timeout = None - no timeout set
    # timeout = float - seconds until timeout is reached
    def set_timeout(self, timeout):
        self.UDPSocket.settimeout(timeout)

    # Write a UDP packet the objects set IPAddress
    def write(self, message):
        self.UDPSocket.sendto(message, (self.IPAddress_Device, self.Port_Device))

    # gets data from the UDP socket
    def receive_udp(self):

        pass

    # Writes a given value to a given register address - constructs message and writes to
    # Register address in hex to write to - starts 0x
    # value to write - hex value to write - starts 0x
    def register_write(self, register_address, value_to_write):
        block_len = str(0x0001)
        UDPMessage = (register_address, " ", block_len, " ", value_to_write)
        UDPFunctions.write(self, UDPMessage)
        pass

    def register_read(self, register_address):
        return 1

    def register_write_verify(self, register_address, value_to_write):
        UDPFunctions.register_write(self, register_address, value_to_write)
        read_value = UDPFunctions.register_read(register_address)
        return value_to_write == read_value

# Define Merlin ADC Processor Board Class - all functions for PC3544m4
class PC3544:
    def __init__(self, switchposition):
        self.switch_pos = int(switchposition)
        self.control_ipaddress = ""
        self.control_port = ""
        self.error_codes = []
        self.error_desc = []

    def get_network_ip(self):
        pass

    def get_network_port(self):
        pass

    def setup_network(self):
        self.network_socket = UDPFunctions(self.control_ipaddress, self.control_port)
        self.network_socket.open()

    def set_gain(self , channel, gain):
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





MADC = UDPFunctions("192.168.1.200","1")
MADC1 = UDPFunctions("192.168.1.201","1")

MADC.open()
MADC.set_timeout(1)
MADC.write("FF")
print(MADC.Port)