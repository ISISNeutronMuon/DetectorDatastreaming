# ISIS Electronics Support Group
# Core Python Scripts
#
# Used to work with IESG hardware within the python environment

import socket #include socket function for network traffic
import pandas as pd
import datetime

Kafka_broker = ""
kafka_event_topic = ""
kafka_runinfo_topic = ""

PC3544_SwitchLookup = ""

# Define a Class for all useful UDP functions the group might use
class UDPFunctions:
    # define initialisation commanda
    def __init__(self,host_ip, host_port, ip_address, port):
        self.IPAddress_Device = ip_address     # set IP to talk to
        self.Port_Device = port                # Set port to use
        self.IPAddress_Host = host_ip
        self.Port_Host = host_port

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

    # Writes a given value to a fiven register address - constructs message and writes to UDP
    def register_write(self, register_address, value_to_write):
        UDPMessage = ""
        UDPFunctions.write(self, UDPMessage)
        pass

    def register_read(self, register_address):
        pass

    def register_write_verify(self, register_address, value_to_write):

        pass

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
MADC.open()
MADC.set_timeout(1)
MADC.write("FF")
print(MADC.Port)