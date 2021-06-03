import collections

import kafka_helper
import datetime
import udp_functions
import data_proc_func
import plot_func
import matplotlib.pyplot as plt
import threading
import queue
import statistics
plt.style.use('ggplot')

# ch_list = [1]
ch_list = list(range(0,24,1))
graph_list = [1 ,1, 0, 0, 0, 0]
# graph_list = [1 ,1, 1, 1, 1, 1]

PACKET_COUNT = 0, 0, 0, 0, 0
stop_threads = False

ProcDataQueue = queue.Queue(maxsize=0)
histogrammed_data_list = [{'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                    'StartSig': collections.Counter(),
                    'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                    'AreaData': collections.Counter()} for i in ch_list]# range(24)]
kafka_dict_list = [{'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                    'StartSig': collections.Counter(),
                    'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                    'AreaData': collections.Counter()} for i in range(24)]

# ######################code to select historic data to process###################################
start_time = datetime.datetime(year=2021, month=6, day=3, hour=12, minute=00)
# start_time = datetime.datetime(year=2021, month=5, day=14, hour=0, minute=00)
# end_time = datetime.datetime(year=2021, month=4, day=29, hour=12, minute=39)
#start_time = (datetime.datetime.now() - datetime.timedelta(minutes=60))
end_time = datetime.datetime.now()
print(end_time)

# ######################python code to select historic data to process###################################
# ####code to process and plot one figure with data on either same or separate plots on historic data to process####
procdata = data_proc_func.kafka_frame_decoder_ip_dict_split_line(start_time, end_time)
plot_func.dict_sel_ch_grph_same(data_proc_func.data_split_dict_channel_ip_combine(procdata, ch_list), ch_list,graph_list)
#plot_func.dict_sel_ch_grph_separ(data_proc_func.data_split_dict_channel_ip_combine(procdata,ch_list), ch_list,graph_list)
plt.show()

exit()


# udp_0_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver,
#                                 args=(HOST_IP, IP_ADDRESS[0], HOST_PORT[0], (lambda: stop_threads), PACKET_COUNT[0], 0))
# udp_0_thread.start()
# udp_1_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver,
#                                 args=(HOST_IP, IP_ADDRESS[1], HOST_PORT[1], (lambda: stop_threads), PACKET_COUNT[1], 1))
# udp_1_thread.start()
# udp_2_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver,
#                                 args=(HOST_IP, IP_ADDRESS[2], HOST_PORT[2], (lambda: stop_threads), PACKET_COUNT[2], 2))
# udp_2_thread.start()
# udp_3_thread = threading.Thread(target=udp_functions.kafka_single_thread_udp_receiver,
#                                 args=(HOST_IP, IP_ADDRESS[3], HOST_PORT[3], (lambda: stop_threads), PACKET_COUNT[3], 3))
# udp_3_thread.start()
# input("Streams starting, press enter to stop streaming threads" + "\n")  # this makes the precess wait for enter press.
# stop_threads = True
# while True:
#     if(udp_0_thread.is_alive() == False):
#         print("thread 0 ended")
#         break