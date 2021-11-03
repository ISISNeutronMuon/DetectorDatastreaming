import threading
import udp_functions
import csv


stop_threads = False
#Open the boards in system .csv file
file = open('MADC_Boards_FESTER.csv')
type(file)
csvreader = csv.reader(file)
#determine header info
header = []
header = next(csvreader)


#Create empty lists for Ports+IPs
ADCName = []
StreamingPorts = []
StreamingIPs = []
#Add the Port/IP info from each row to the master lists
for row in csvreader:

    ADCName.append(row[0])

    StreamingPorts.append(row[6])
    StreamingPorts.append(row[8])
    StreamingPorts.append(row[10])
    StreamingPorts.append(row[12])

    StreamingIPs.append(row[5])
    StreamingIPs.append(row[7])
    StreamingIPs.append(row[9])
    StreamingIPs.append(row[11])

#print outcome
print("ADC Names",ADCName)
print("Data Streaming Ports in use:",StreamingPorts)
print("Data Streaming IPs in use:",StreamingIPs)
print("No. Streaming ADC's:",len(ADCName))
print("No. Streaming Ports:",len(StreamingPorts))

#Create Empty Packet Count List
PACKET_COUNT = []
for i in range(len(StreamingPorts)):
    PACKET_COUNT.append("0")


for i in range(len(StreamingPorts)):
  print("Thread Create, PORT:",StreamingPorts[i]," IP:", StreamingIPs[i])
  udp_thread = threading.Thread(target=udp_functions.MultipleStreamToProcessedEV42, args=((lambda: stop_threads), PACKET_COUNT[i], i, int(StreamingPorts[i]), StreamingIPs[i]))
  udp_thread.start()

input("Streams Started, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
stop_threads = True
while True:
    if (udp_thread.is_alive() == False):
        print("Threads Closed")
        break

