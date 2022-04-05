import IESG_Core
import threading
import csv


if __name__ == "__main__":
    stop_threads = False
    # Open the boards in system .csv file
    file = open('MADC_Boards.csv')
    type(file)
    boards = csv.reader(file)
    # determine header info
    header = []
    header = next(boards)

    # Create empty lists for Ports+IPs
    ADCName = []
    StreamingPorts = []
    StreamingIPs = []
    # Add the Port/IP info from each row to the master lists
    for row in boards:
        ADCName.append(row[0])

        StreamingPorts.append(row[6])
        StreamingPorts.append(row[8])
        StreamingPorts.append(row[10])
        StreamingPorts.append(row[12])

        StreamingIPs.append(row[5])
        StreamingIPs.append(row[7])
        StreamingIPs.append(row[9])
        StreamingIPs.append(row[11])

    # print outcome
    print("Boards configuration read complete:")
    print("ADC Names", ADCName)
    print("Data Streaming Ports in use:", StreamingPorts)
    print("Data Streaming IPs in use:", StreamingIPs)
    print("No. Streaming ADC's:", len(ADCName))
    print("No. Streaming Ports:", len(StreamingPorts))

    PACKET_COUNT = []
    for i in range(len(StreamingPorts)):
        PACKET_COUNT.append("0")
    lock = threading.Lock()


    streaming_object =[]

    # creating streaming threads
    for i in range(len(StreamingPorts)):
        print("Thread Create, PORT:", StreamingPorts[i], " IP:", StreamingIPs[i])
        streaming_object[i] = IESG_Core.dae_streamer(stream_ip=StreamingIPs[i], stream_port=StreamingPorts[i],
                                                     kafka_broker="livedata.isis.cclrc.ac.uk",
                                                     influxdb_database="python_testing")

        thread_name = "Kafka Stream Handler Thread " + i + "-"+ StreamingIPs[i]
        processing_thread = threading.Thread(target=streaming_object[i].stream_loop_to_kafka(),
                                             args=((lambda: stop_threads), lock, thread_name))
        processing_thread.start()

    input(
        "Streams Started, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
    stop_threads = True
    while True:
        if processing_thread.is_alive() == False:
            print("Threads Closed")
            break
