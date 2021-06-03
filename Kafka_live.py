import collections
import kafka_helper
import udp_functions
import plot_func
import matplotlib.pyplot as plt
import threading
import queue
plt.style.use('ggplot')

# ch_list = [0,1,2,3]
ch_list = list(range(0,24,1))
graph_list = [1,1,1,1,1,1]
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

if sum(graph_list) > 3:
    number_of_cols = 3
    number_of_rows = 2
else:
    number_of_cols = sum(graph_list)
    number_of_rows = 1
figure, axes = plt.subplots(nrows=number_of_rows, ncols=number_of_cols)

# ######################START THE UDP KAFKA THREADS#########################################
thread_list = udp_functions.thread_ch_list(ch_list)
for i in thread_list:
    udp_thread = threading.Thread(target=udp_functions.kafka_slim_single_thread_udp_receiver,
                                  args=((lambda: stop_threads), PACKET_COUNT[i], i))
    udp_thread.start()


def data_split_live_dict_v4(kafka_data):
    HEADER_STRING = "ffffffffffffffff0"
    END_HEADER = "efffffffffffffff0"
    position = [[] for i in range(24)]
    PulseHeight = [[] for i in range(24)]
    StartSig = [[] for i in range(24)]
    Misplace = [[] for i in range(24)]
    MaxSlope = [[] for i in range(24)]
    AreaData = [[] for i in range(24)]
    kafka_dict_list = [{'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                        'StartSig': collections.Counter(),
                        'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                        'AreaData': collections.Counter()} for i in range(24)]

    #format data
    packet_data_list = kafka_data.get("packet").replace(HEADER_STRING, "\n" + HEADER_STRING) \
        .replace(END_HEADER, "\n" + END_HEADER).splitlines()
    channel_offset = (int(kafka_data.get("packet_info")[11:]) - 1) * 6

    list_length = len(packet_data_list)
    for j in range(1, list_length):
        line = packet_data_list[j][128:]
        kafka_data_length = (len(line))
        for i in range(0, kafka_data_length, 32):  # return 32 character chunks
            channel = int(((bin(int('1' + (line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
            position[channel].append(int(line[i + 29:i + 32], 16)/8)
            PulseHeight[channel].append(int(line[i + 26:i + 29], 16)/8)
            StartSig[channel].append(int(line[i + 23:i + 26], 16))
            Misplace[channel].append(int(line[i + 20:i + 23], 16))
            MaxSlope[channel].append(int(line[i + 17:i + 20], 16))
            AreaData[channel].append(int(line[i + 14:i + 17], 16))
        for i in range(0, 24, 1):
            if len(position[i]):
                kafka_dict_list[i]['position'] = collections.Counter(position[i])
                kafka_dict_list[i]['PulseHeight'] = collections.Counter(PulseHeight[i])
                kafka_dict_list[i]['StartSig'] = collections.Counter(StartSig[i])
                kafka_dict_list[i]['Misplace'] = collections.Counter(Misplace[i])
                kafka_dict_list[i]['MaxSlope'] = collections.Counter(MaxSlope[i])
                kafka_dict_list[i]['AreaData'] = collections.Counter(AreaData[i])
    ProcDataQueue.put(kafka_dict_list)


kafka_helper.do_func_on_live_data(data_split_live_dict_v4)

plt.ion()
plt.show()

while True:
    while ProcDataQueue.qsize() > 0: # or (ProcDataQueue_list[1].qsize() > 0):
        print(ProcDataQueue.qsize())
        kafka_dict_deque = ProcDataQueue.get()
        for i in ch_list:
            if len(kafka_dict_deque[i]['position']):
                histogrammed_data_list[i]['position'] = collections.Counter(histogrammed_data_list[i]['position']) + \
                                                        collections.Counter(kafka_dict_deque[i]['position'])
                histogrammed_data_list[i]['PulseHeight'] = collections.Counter(histogrammed_data_list[i]['PulseHeight']) + \
                                                           collections.Counter(kafka_dict_deque[i]['PulseHeight'])
                histogrammed_data_list[i]['StartSig'] = collections.Counter(histogrammed_data_list[i]['StartSig']) + \
                                                        collections.Counter(kafka_dict_deque[i]['StartSig'])
                histogrammed_data_list[i]['Misplace'] = collections.Counter(histogrammed_data_list[i]['Misplace']) + \
                                                        collections.Counter(kafka_dict_deque[i]['Misplace'])
                histogrammed_data_list[i]['MaxSlope'] = collections.Counter(histogrammed_data_list[i]['MaxSlope']) + \
                                                        collections.Counter(kafka_dict_deque[i]['MaxSlope'])
                histogrammed_data_list[i]['AreaData'] = collections.Counter(histogrammed_data_list[i]['AreaData']) + \
                                                        collections.Counter(kafka_dict_deque[i]['AreaData'])
        figure, axes = plot_func.callable_dict_sel_ch_grph_same(ch_list, graph_list, histogrammed_data_list, figure, axes)
    plt.pause(1)
    plt.draw()
exit()


# ############code to read data kafka data and sort into frames###################################
# position_histo = collections.Counter()
# kafka_data_dict = {'position': collections.Counter()}
# V2
# ProcDataQueue = queue.Queue(maxsize=0)
# histogrammed_data = {'position': collections.Counter(), 'PulseHeight': collections.Counter()}
# V3
# ProcDataQueue_list = [queue.Queue(maxsize=0) for i in range(24)]

# def data_split_live_dict_v1(kafka_data):
#     position = []
#     line = kafka_data.get("packet")[128:]
#     kafka_data_length = (len(line))
#     for i in range(0, kafka_data_length, 32):  # return 32 character chunks
#         position.append(int(line[i + 29:i + 32], 16))
#     ProcDataQueue.put(collections.Counter(position))


# def data_split_live_dict_v2(kafka_data):
#     position, PulseHeight = [], []
#     line = kafka_data.get("packet")[128:]
#     kafka_data_length = (len(line))
#     for i in range(0, kafka_data_length, 32):  # return 32 character chunks
#         position.append(int(line[i + 29:i + 32], 16))
#         PulseHeight.append(int(line[i + 26:i + 29], 16))
#     ProcDataQueue.put(collections.Counter(position))
#     ProcDataQueue.put(collections.Counter(PulseHeight))


# def data_split_live_dict_v3(kafka_data):
#     position = [[] for i in range(24)]
#     PulseHeight = [[] for i in range(24)]
#     # channel_offset = (int(list(kafka_data.keys())[l][11:]) - 1) * 6
#     line = kafka_data.get("packet")[128:]
#     kafka_data_length = (len(line))
#     for i in range(0, kafka_data_length, 32):  # return 32 character chunks
#         channel = int(((bin(int('1' + (line[i + 11:i + 13]), 16))[3:])[3:6]), 2)  # + channel_offset
#         position[channel].append(int(line[i + 29:i + 32], 16))
#         PulseHeight[channel].append(int(line[i + 26:i + 29], 16))
#     for i in range(0, 24, 1):
#         ProcDataQueue_list[i].put(collections.Counter(position[i]))
#         ProcDataQueue_list[i].put(collections.Counter(PulseHeight[i]))
# while ProcDataQueue.qsize() > 0:
#     print(ProcDataQueue.qsize())
#     position_histo = position_histo + ProcDataQueue.get()
# axes.bar(list(position_histo.keys()), position_histo.values(), color='red', width=1)

# V2
# while ProcDataQueue.qsize() > 0:
#     print(ProcDataQueue.qsize())
# histogrammed_data['position'] = collections.Counter(histogrammed_data['position']) + \
#                                 (ProcDataQueue.get())
# histogrammed_data['PulseHeight'] = collections.Counter(histogrammed_data['PulseHeight']) + \
#                                 (ProcDataQueue.get())
# axes[0].bar(list(histogrammed_data['position'].keys()), histogrammed_data['position'].values(), color='red',
#             width=1)
# axes[1].bar(list(histogrammed_data['PulseHeight'].keys()), histogrammed_data['PulseHeight'].values(),
#             color='red')  # , width=1)

# V3
# while (ProcDataQueue_list[0].qsize() > 0) or (ProcDataQueue_list[1].qsize() > 0):
#     print(ProcDataQueue_list[0].qsize())
#     print(ProcDataQueue_list[1].qsize())
#     for i in range(0, 24, 1):
#         histogrammed_data_list[i]['position'] = collections.Counter(histogrammed_data_list[i]['position']) + \
#                                            (ProcDataQueue_list[i].get())
#         histogrammed_data_list[i]['PulseHeight'] = collections.Counter(histogrammed_data_list[i]['PulseHeight']) + \
#                                               (ProcDataQueue_list[i].get())
# axes[0,0].bar(list(histogrammed_data_list[0]['position'].keys()), histogrammed_data_list[0]['position'].values(), color='red',
#             width=1)
# axes[1,0].bar(list(histogrammed_data_list[0]['PulseHeight'].keys()), histogrammed_data_list[0]['PulseHeight'].values(),
#             color='red')  # , width=1)
# axes[0, 1].bar(list(histogrammed_data_list[1]['position'].keys()), histogrammed_data_list[1]['position'].values(),
#                color='red',
#                width=1)
# axes[1, 1].bar(list(histogrammed_data_list[1]['PulseHeight'].keys()),
#                histogrammed_data_list[1]['PulseHeight'].values(),
#                color='red')  # , width=1)
# axes[0, 0].bar(list(histogrammed_data_list[0]['position'].keys()), histogrammed_data_list[0]['position'].values(),
#                color='red',
#                width=1)
# axes[1, 0].bar(list(histogrammed_data_list[0]['PulseHeight'].keys()),
#                histogrammed_data_list[0]['PulseHeight'].values(),
#                color='red')  # , width=1)
# axes[0, 1].bar(list(histogrammed_data_list[1]['position'].keys()), histogrammed_data_list[1]['position'].values(),
#                color='red',
#                width=1)
# axes[1, 1].bar(list(histogrammed_data_list[1]['PulseHeight'].keys()),
#                histogrammed_data_list[1]['PulseHeight'].values(),
#                color='red')  # , width=1)
# plot_func.legend_without_duplicate_labels(axes[0,0])
# handles, labels = axes[0, 0].get_legend_handles_labels()
# if len(labels) != 0:
#     axes[0,0].legend(handles, labels[-1:])