import kafka_helper
import datetime
import udp_functions
import data_proc_func
import plot_func
import matplotlib.pyplot as plt
import threading

plt.style.use('ggplot')
PACKET_COUNT = 0, 0, 0, 0, 0
HOST_PORT = 48641, 48642, 48643, 48644, 48868
HOST_IP = "192.168.1.125"
IP_ADDRESS = "192.168.1.201", "192.168.1.202", "192.168.1.203", "192.168.1.204", "192.168.1.251"
stop_threads = False

# ############code to read data kafka data and sort into frames###################################
# ip_data = kafka_helper.do_func_on_live_data(data_proc_func.live_data_test)
# ip_data = kafka_helper.do_func_on_live_data(data_proc_func.kafka_live_data_proc)
#
# ip_data = kafka_helper.do_func_on_live_data(data_proc_func.kafka_data_decoder_ip_dict)
# ip_data = kafka_helper.do_func_on_live_data(data_proc_func.data_split_dict(data_proc_func.kafka_data_decoder_ip_dict))
# while True:
#     print(ip_data)
    # break


###########code to start sending data streams to kafka streams###################################
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
# ################################################################################################

# ######################code to select historic data to process###################################
start_time = datetime.datetime(year=2021, month=3, day=23, hour=13, minute=30)
end_time = datetime.datetime(year=2021, month=3, day=23, hour=14, minute=00)
# start_time = (datetime.datetime.now() - datetime.timedelta(minutes=20))
# end_time = datetime.datetime.now()
##################################################################################################

# ######################python code to select historic data to process###################################
# procdata = udp_functions.kafka_frame_decoder_ip_dict(start_time, end_time)
# for i in procdata:
#     # udp_functions.plt_dict_sel_ch(udp_functions.data_split_dict_channel(procdata[i]), i, [0,1,2,3,4,5])
#     udp_functions.plt_dict_sel_ch_graph(udp_functions.data_split_dict_channel(procdata[i]), i, [0, 1, 2, 3, 4, 5],
#                                         [1, 1, 1, 1, 1, 0])
# plt.show()
# ################################################################################################


# ####code to process and plot one figure with data on either same or separate plots on historic data to process####
procdata = data_proc_func.kafka_frame_decoder_ip_dict_split_line(start_time, end_time)
plot_func.dict_sel_ch_grph_same(data_proc_func.data_split_dict_channel_ip_combine(procdata), [1,2,5,6,7,11,13,14,17,18,20,23],
                                [1, 1, 0, 0, 0, 0])
# plot_functions.dict_sel_ch_grph_separ(udp_functions.data_split_dict_channel_ip_combine(procdata), list(range(0,24)),[1,1,1,1,1,1])
plt.show()

exit()


