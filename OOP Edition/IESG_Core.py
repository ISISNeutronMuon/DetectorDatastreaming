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
import socket  # include socket function for network traffic
import struct  # used to encode data for UDP sender
import time

import pandas as pd  # include pandas for CSV reading and any array functions
import datetime  # include date time to get date time values as needed.

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
        self.UDPSocket = None               # generic socket for any use
        self.UDPSocket_write = None         # define the UDP socket object - writing data
        self.UDPSocket_receive = None       # define the UDP socket object - receiving data
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
        UDPFunctions.open(self, port_type="receive")        # open ports
        UDPFunctions.open(self, port_type="read_command")
        self.UDPSocket_read_command.sendto(message, (self.IPAddress_Device, self.ports_read_command["device"])) # send read command
        data, address = self.UDPSocket_receive.recvfrom(1024)       # receive byte array with the data
        UDPFunctions.close(self, port_type="receive")               # close ports
        UDPFunctions.close(self, port_type="read_command")
        returned = data.hex()              # convert byte array into hex string
        returned = "0x" + returned[12:]    # remove read command from data
        return returned                    # return the read register value

    # Writes a given value to  given register address and then checks if it was written correctly
    def register_write_verify(self, register_address, value_to_write, delay=0):
        value_to_write_len = len(value_to_write)
        block_size = int((value_to_write_len-2) / 8) + ((value_to_write_len - 2) % 8 > 0)  # calc block size

        UDPFunctions.register_write(self, register_address, value_to_write, delay=delay) # write the data
        time.sleep(delay)
        read_value = UDPFunctions.register_read(self, register_address=register_address, block_size=block_size) # read register
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

        self.control_ipaddress = self.MADC_IPs["BE_FPGA_IP"]        # Get BE/Control IP from dict
        self.control_port_W = self.MADC_Ports["BE_FPGA_PORT_W"]     # Get BE/Control port from dict
        self.control_port_R = self.MADC_Ports["BE_FPGA_PORT_R"]     # Get BE/Control port from dict
        self.AddressMap = self.get_reg_address_map()                # Get the Address Map Information
        self.control_socket = self.setup_control_network()          # setup control socket

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
        success_list = []   # list to hold if each register write type completed correctly
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
                Verify_Status = self.control_socket.register_write_verify(register_address=Register, value_to_write=gain)
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
            GainBlocks = [("0x000000" + gain[(i*2)+2:(i*2)+4]) for i in range(4)]
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
        return all(success_list)   # return true if everything is successful


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
            print("\033[91m\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num],
                  "- (", self.ErrorNumberList[error_num], ")", self.ErrorDescList[error_num], "\033[0m")
        elif self.ErrorSeverity[error_num] == "WARNING":
            print("\033[93m\033[1mIESG_Error_Handler: ", self.ErrorSeverity[error_num],
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


Error = ErrorHandler()  # create error handler object to hold all errors within
MADC = [PC3544(0) for i in range(1)]

if __name__ == "__main__":
    starttime = time.time()
    ADC = PC3544(0)
    # ADC.set_gain("6B", "0x12345678",WriteType="Write", write_FPGA=False, write_flash=True)
    for channel in range(24):
        A_Channel = str(channel) + "A"
        B_Channel = str(channel) + "B"
        print(A_Channel+": "+str(ADC.set_gain(A_Channel, "0xbeef", WriteType="Verify", write_FPGA=True, write_flash=True)) +
              " "+B_Channel+": "+str(ADC.set_gain(B_Channel, "0xcafe", WriteType="Verify", write_FPGA=True, write_flash=True)))
    print("all gain value written, took: ", time.time() - starttime)
    Error.print_all()
