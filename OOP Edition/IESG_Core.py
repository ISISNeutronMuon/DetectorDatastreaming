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
import struct           # used to encode data for UDP sender
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
        self.UDPSocket.bind((self.IPAddress_Host, self.Port_Host))

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
        self.open()
        self.UDPSocket.sendto(struct.pack('!q', message), (self.IPAddress_Device, self.Port_Device))

        self.close()

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

    #on MADC object creation
    def __init__(self, switchposition):
        if switchposition in range(32):             # Validate inputted switch position - within 0-31
            self.switch_pos = int(switchposition)   # If valid get objects switch pos
        else:                                       # If incorrect add error to error list
            Error.AddError(17, "Value Range Error - Switch Position is out of range, set to 0")
            self.switch_pos = 0                     # Default to pos 0

        self.MADC_IPs = self.get_network_ip()        # Get IP info from the Switch Position
        self.MADC_Ports = self.get_network_port()    # Get port info from the Switch Position

        self.control_ipaddress = self.MADC_IPs["BE_FPGA_IP"]    # Get BE/Control IP from dict
        self.control_port = self.MADC_Ports["BE_FPGA_PORT"]     # Get BE/Control port from dict

    # Function to get the MADC's 5 IP addresses from its switch position
    def get_network_ip(self):
        Possible_BE_IP = [148, 149, 150, 155, 156, 157, 158, 159, 160, 165, 166, 167, 168, 169, 170, 175,
                          176, 177, 178, 179, 180, 185, 186, 187, 188, 189, 190, 195, 196, 197, 198, 199]
        BE_FPGA_IP = "192.168.1." + str(Possible_BE_IP[self.switch_pos])        # BE IP = 192.168.1. Matching end number
        FE_FPGA0_IP = "192.168.2." + str(100+(self.switch_pos * 4))             # Calc FE FPGA0 IP
        FE_FPGA1_IP = "192.168.2." + str(101+(self.switch_pos * 4))             # Calc FE FPGA1 IP
        FE_FPGA2_IP = "192.168.2." + str(102+(self.switch_pos * 4))             # Calc FE FPGA2 IP
        FE_FPGA3_IP = "192.168.2." + str(103+(self.switch_pos * 4))             # Calc FE FPGA3 IP
        return {"BE_FPGA_IP" : BE_FPGA_IP,"FE_FPGA0_IP" : FE_FPGA0_IP,"FE_FPGA1_IP" : FE_FPGA1_IP,
                "FE_FPGA2_IP" : FE_FPGA2_IP,"FE_FPGA3_IP" : FE_FPGA3_IP}   # return the Addresses as a dictionary

    # Get the network ports the MADC uses from the switch positon
    def get_network_port(self):
        BE_FPGA_PORT = 0                                # Set BE as port 0 - currently not used
        FE_FPGA0_PORT = 48640 + (self.switch_pos * 4)   # Calc FE FPGA0 port number
        FE_FPGA1_PORT = 48641 + (self.switch_pos * 4)   # Calc FE FPGA1 port number
        FE_FPGA2_PORT = 48642 + (self.switch_pos * 4)   # Calc FE FPGA2 port number
        FE_FPGA3_PORT = 48643 + (self.switch_pos * 4)   # Calc FE FPGA3 port number
        return {"BE_FPGA_PORT" : BE_FPGA_PORT, "FE_FPGA0_PORT" : FE_FPGA0_PORT, "FE_FPGA1_PORT" : FE_FPGA1_PORT,
                "FE_FPGA2_PORT" : FE_FPGA2_PORT, "FE_FPGA3_PORT" : FE_FPGA3_PORT} # Return ports as a dictionary

    # Configure a network socket for the control connection to the MADC
    def setup_control_network(self):
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

# Error Handler class - define once on program start to be able to log all errors within the program
class ErrorHandler:
    def __init__(self):
        self.ErrorNumberList = []
        self.ErrorDescList = []
        print(self.ErrorDescList)

    # Checks to see if the current Error list is valid
    def CheckErrors_Valid(self):
        if len(self.ErrorNumberList) != len(self.ErrorDescList):
            print("Error List Length Mismatch")
            return False

    def AddError(self, ErrorNumber = 0, ErrorDesc = "unknown error has occured (default)" , printToUser = False):
        self.ErrorNumberList.append(ErrorNumber)
        self.ErrorDescList.append(ErrorDesc)
        self.CheckErrors_Valid()
        if printToUser:
            print("An error has occured! - (", ErrorNumber, ")", ErrorDesc)

    # Function to print all errors to terminal, returns false if an errors are invalid, true if printed
    def PrintAll(self):
        if self.CheckErrors_Valid() == False:
            return False
        if len(self.ErrorNumberList) == 0:
            print("Program currently has 0 errors")
            return True
        else:
            print("Program currently has ", len(self.ErrorNumberList), " errors")
        for Error in range(len(self.ErrorNumberList)):
            print("Error ", Error, ": ", self.ErrorNumberList[Error], " - ", self.ErrorDescList[Error])
        return True

    # Function to print last errors to terminal, returns false if an errors are invalid, true if printed
    def PrintLast(self):
        numErrors = len(self.ErrorNumberList)
        if self.CheckErrors_Valid() == False:
            return False
        if numErrors == 0:
            print("Program currently has 0 errors")
            return True
        else:
            print("Program currently has ", numErrors, " errors")
            print("The last error was: ", self.ErrorNumberList[numErrors], " - ", self.ErrorDescList[numErrors])


Error = ErrorHandler()
MADC = []
if __name__ == "__main__":
    UDPTest = UDPFunctions("130.246.49.202",10000,"192.168.1.200",10001)
    UDPTest.register_write(0x0, 1)

