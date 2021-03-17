import kafka_helper
import datetime
import udp_functions
import matplotlib.pyplot as plt
import sys
import socket
import threading
import numpy
import collections

PACKET_COUNT = 0, 0, 0, 0, 0
HOST_PORT = 48641, 48642, 48643, 48644, 48868
HOST_IP = "192.168.1.125"
IP_ADDRESS = "192.168.1.201", "192.168.1.202", "192.168.1.203", "192.168.1.204", "192.168.1.251"
stop_threads = False
FILE_PATH = 'I:/LabVIEW/DJT/ADC_data_analysis/data/test.txt'
FILE0 = open(FILE_PATH, "w")
combined_data = {'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                 'StartSig': collections.Counter(),
                 'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                 'AreaData': collections.Counter()}

# my_data = kafka_helper.get_data_between(datetime.datetime.now() - datetime.timedelta(minutes=180),
#                                        datetime.datetime.now())
# element = (my_data[0].get("packet"))


# ############code to read data kafka data and sort into frames###################################
# kafka_helper.do_func_on_live_data(print)
# udp_1_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[1],
#                                                                                           HOST_PORT[1],
#                                                                                           (lambda: stop_threads),
#                                                                                           PACKET_COUNT[1], 1))
# udp_1_thread.start()
# udp_2_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver, args=(HOST_IP, IP_ADDRESS[2],
#                                                                                           HOST_PORT[2],
#                                                                                           (lambda: stop_threads),
#                                                                                           PACKET_COUNT[2], 2))
# udp_2_thread.start()
# input("Streams starting, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
# stop_threads = True
# ################################################################################################


# ############code to read data kafka data and sort into frames###################################
start_time = datetime.datetime(year=2021, month=3, day=17, hour=10, minute=10)
end_time = datetime.datetime(year=2021,month=3,day=17,hour=10,minute=40)
# #start_time = (datetime.datetime.now() - datetime.timedelta(minutes=210))
# #end_time = datetime.datetime.now()
procdata = udp_functions.kafka_frame_decoder_ip(start_time, end_time)
# ################################################################################################


# ############code to split data into dictionary and add to existing dictionary and plot##########
# # this function sorts the data and returns a dictionary frequency
# latest_data = udp_functions.dict_data_split(procdata[1:])
# # add the new data to the existing data
# combined_data = udp_functions.dict_add(combined_data, latest_data)
# figure, axes = plt.subplots(nrows=2, ncols=3)
# axes[0, 0].bar(list(combined_data['position'].keys()), combined_data['position'].values())
# axes[0, 0].set_title('Position')
# axes[0, 1].bar(list(combined_data['PulseHeight'].keys()), combined_data['PulseHeight'].values())
# axes[0, 1].set_title('Pulse Height')
# axes[0, 2].bar(list(combined_data['StartSig'].keys()), combined_data['StartSig'].values())
# axes[0, 2].set_title('Start Sig')
# axes[1, 0].bar(list(combined_data['Misplace'].keys()), combined_data['Misplace'].values())
# axes[1, 0].set_title('Misplace')
# axes[1, 1].bar(list(combined_data['MaxSlope'].keys()), combined_data['MaxSlope'].values())
# axes[1, 1].set_title('Max Slope')
# axes[1, 2].bar(list(combined_data['AreaData'].keys()), combined_data['AreaData'].values())
# axes[1, 2].set_title('Area Data')
# plt.show()
################################################################################################


# udp_functions.data_plot_string(procdata[1:])
# combined_data = udp_functions.data_split(procdata[1:])
# axes[0, 3].bar(list(combined_data[6].keys()), combined_data[6].values())
# axes[0, 3].set_title('Time')
# axes[0, 0].hist(combined_data[0], bins=255)
# axes[0, 0].bar(combined_data[0], bins=255)
# axes[0, 0].set_title('Position')
# axes[0, 1].hist(combined_data[1], bins=255)
# axes[0, 1].set_title('Pulse Height')
# axes[0, 2].hist(combined_data[2], bins=255)
# axes[0, 2].set_title('Start Sig')
# axes[1, 0].hist(combined_data[3], bins=255)
# axes[1, 0].set_title('Misplace')
# axes[1, 1].hist(combined_data[4], bins=255)
# axes[1, 1].set_title('Max Slope')
# axes[1, 2].hist(combined_data[5], bins=255)
# axes[1, 2].set_title('Area Data')
# axes[0, 3].hist(combined_data[6])
# axes[0, 3].set_title('Time')
# plt.show()

# print(procdata)
# FILE0.write(procdata)
# FILE0.close()
exit()
