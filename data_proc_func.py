import collections
import queue

import numpy as np

import kafka_helper
import plot_func

HEADER_STRING = "ffffffffffffffff0"
END_HEADER = "efffffffffffffff0"


def kafka_frame_decoder(start_time, end_time):
    processed_data = ""
    current_data = ""
    i = 0
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    for i in range(0, list_length):
        current_data = (all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        processed_data = processed_data + current_data
    return processed_data


#######Gets historic data from kafka and returns separate dictionaries entry for each line ####################
def kafka_frame_decoder_ip_nest_dict(start_time, end_time):
    current_data = {'packet': "", 'packet_info': ""}
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    processed_data = [{'packet': "", 'packet_info': ""} for i in range(list_length)]
    for i in range(0, list_length):
        current_data['packet'] = (all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        current_data['packet_info'] = all_data[i].get("packet_info")
        processed_data[i].update(current_data)
    return processed_data


#############Gets historic data from kafka and returns separate dictionaries of each IP addresses#######
def kafka_frame_decoder_ip_dict(start_time, end_time):
    current_data = {'packet': "", 'packet_info': ""}
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    ip_data = {}
    for i in range(0, list_length):
        current_data['packet'] = (all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
            .replace(END_HEADER, "\n" + END_HEADER)
        current_data['packet_info'] = all_data[i].get("packet_info")
        if current_data.get("packet_info") in ip_data:
            ip_data[current_data.get("packet_info")] = ip_data[current_data.get("packet_info")] \
                                                       + current_data.get("packet")
        else:
            ip_data[current_data.get("packet_info")] = current_data.get("packet")
    return ip_data


#############Gets historic data from kafka and returns separate dictionaries of each IP addresses#######
def kafka_frame_decoder_ip_dict_split_line(start_time, end_time):
    current_data = {'packet': "", 'packet_info': ""}
    all_data = kafka_helper.get_data_between(start_time, end_time)
    list_length = len(all_data)
    ip_data = {}
    for i in range(0, list_length):
        current_data['packet'] = ((all_data[i].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
                                  .replace(END_HEADER, "\n" + END_HEADER))
        current_data['packet_info'] = all_data[i].get("packet_info")
        current_data['packet'] = current_data.get("packet").splitlines()  # turn carriage return string into a list
        current_data['packet'] = [e[128:] for e in current_data.get("packet")]  # remove the header from the list
        if current_data.get("packet_info") in ip_data:
            ip_data[current_data.get("packet_info")] = ip_data[current_data.get("packet_info")] \
                                                       + current_data.get("packet")
        else:
            ip_data[current_data.get("packet_info")] = current_data.get("packet")
    return ip_data


#############Works on data live from kafka and returns separate dictionaries of each IP addresses#######
def kafka_data_decoder_ip_dict(all_data):
    current_data = {'packet': "", 'packet_info': ""}
    ip_data = {}
    current_data['packet'] = (all_data[0].get("packet")).replace(HEADER_STRING, "\n" + HEADER_STRING) \
        .replace(END_HEADER, "\n" + END_HEADER)
    current_data['packet'] = current_data.get("packet").splitlines()  # turn carriage return string into a list
    current_data['packet'] = [e[128:] for e in current_data.get("packet")]  # remove the header from the list
    current_data['packet_info'] = all_data[0].get("packet_info")
    return current_data


def data_split_string(kafka_data):
    position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData = [], [], [], [], [], []
    Time = []
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[128:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        kafka_data_length = (len(line))
        for i in range(0, kafka_data_length, 32):  # return 32 character chunks
            position.append(int(line[i + 29:i + 32], 16))
            PulseHeight.append(int(line[i + 26:i + 29], 16))
            StartSig.append(int(line[i + 23:i + 26], 16))
            Misplace.append(int(line[i + 20:i + 23], 16))
            MaxSlope.append(int(line[i + 17:i + 20], 16))
            AreaData.append(int(line[i + 14:i + 17], 16))
            Time.append(int(line[i + 3:i + 8], 16))
    return position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData, Time


def data_split_dict(kafka_data):
    position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData = [], [], [], [], [], []
    kafka_data_dict = {'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                       'StartSig': collections.Counter(),
                       'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                       'AreaData': collections.Counter()}
    # if not kafka_data:
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[128:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        kafka_data_length = (len(line))
        for i in range(0, kafka_data_length, 32):  # return 32 character chunks
            position.append(int(line[i + 29:i + 32], 16))
            PulseHeight.append(int(line[i + 26:i + 29], 16))
            StartSig.append(int(line[i + 23:i + 26], 16))
            Misplace.append(int(line[i + 20:i + 23], 16))
            MaxSlope.append(int(line[i + 17:i + 20], 16))
            AreaData.append(int(line[i + 14:i + 17], 16))
    kafka_data_dict['position'] = collections.Counter(position)
    kafka_data_dict['PulseHeight'] = collections.Counter(PulseHeight)
    kafka_data_dict['StartSig'] = collections.Counter(StartSig)
    kafka_data_dict['Misplace'] = collections.Counter(Misplace)
    kafka_data_dict['MaxSlope'] = collections.Counter(MaxSlope)
    kafka_data_dict['AreaData'] = collections.Counter(AreaData)
    return kafka_data_dict


def data_split_dict_channel(kafka_data):
    position = [[] for i in range(6)]
    PulseHeight = [[] for i in range(6)]
    StartSig = [[] for i in range(6)]
    Misplace = [[] for i in range(6)]
    MaxSlope = [[] for i in range(6)]
    AreaData = [[] for i in range(6)]
    kafka_dict_list = [{'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                        'StartSig': collections.Counter(),
                        'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                        'AreaData': collections.Counter()} for i in range(6)]

    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[128:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        if line:
            kafka_data_length = (len(line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                channel = int(((bin(int('1' + (line[i + 11:i + 13]), 16))[3:])[3:6]), 2)
                # print(line)
                # print(channel)
                position[channel].append(int(line[i + 29:i + 32], 16))
                PulseHeight[channel].append(int(line[i + 26:i + 29], 16))
                StartSig[channel].append(int(line[i + 23:i + 26], 16))
                Misplace[channel].append(int(line[i + 20:i + 23], 16))
                MaxSlope[channel].append(int(line[i + 17:i + 20], 16))
                AreaData[channel].append(int(line[i + 14:i + 17], 16))
    for i in range(0, 6, 1):
        kafka_dict_list[i]['position'] = collections.Counter(position[i])
        kafka_dict_list[i]['PulseHeight'] = collections.Counter(PulseHeight[i])
        kafka_dict_list[i]['StartSig'] = collections.Counter(StartSig[i])
        kafka_dict_list[i]['Misplace'] = collections.Counter(Misplace[i])
        kafka_dict_list[i]['MaxSlope'] = collections.Counter(MaxSlope[i])
        kafka_dict_list[i]['AreaData'] = collections.Counter(AreaData[i])
    return kafka_dict_list


def data_split_dict_channel_ip_combine(kafka_data, ch_list):
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
    l = 0
    for line in kafka_data.values():
        channel_offset = (int(
            list(kafka_data.keys())[l][11:]) - 1) * 6  # create and offset for channel based on ipaddress
        l = l + 1
        for list_line in line:
            kafka_data_length = (len(list_line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                if list_line[i:i + 32] != '00000000000000000000000000000000':  # to remove empty packets
                    channel = int(((bin(int('1' + (list_line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
                    if channel in ch_list:
                        position[channel].append(int(int(list_line[i + 29:i + 32], 16) / 16))
                        PulseHeight[channel].append(int(int(list_line[i + 26:i + 29], 16) / 16))
                        StartSig[channel].append(int(int(list_line[i + 23:i + 26], 16) / 16))
                        Misplace[channel].append(int(int(list_line[i + 20:i + 23], 16) / 16))
                        MaxSlope[channel].append(int(int(list_line[i + 17:i + 20], 16) / 16))
                        AreaData[channel].append(int(int(list_line[i + 14:i + 17], 16) / 16))
    for i in ch_list:  # range(0, 24, 1):
        kafka_dict_list[i]['position'] = collections.Counter(position[i])
        kafka_dict_list[i]['PulseHeight'] = collections.Counter(PulseHeight[i])
        kafka_dict_list[i]['StartSig'] = collections.Counter(StartSig[i])
        kafka_dict_list[i]['Misplace'] = collections.Counter(Misplace[i])
        kafka_dict_list[i]['MaxSlope'] = collections.Counter(MaxSlope[i])
        kafka_dict_list[i]['AreaData'] = collections.Counter(AreaData[i])
    return kafka_dict_list


def data_split_dict_channel_ip_combine_time(kafka_data, ch_list):
    time_bins = 1024
    position = [[[] for i in range(time_bins)] for t in range(24)]
    PulseHeight = [[[] for i in range(time_bins)] for t in range(24)]
    StartSig = [[[] for i in range(time_bins)] for t in range(24)]
    Misplace = [[[] for i in range(time_bins)] for t in range(24)]
    MaxSlope = [[[] for i in range(time_bins)] for t in range(24)]
    AreaData = [[[] for i in range(time_bins)] for t in range(24)]
    kafka_time_dict_list = [[{'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                              'StartSig': collections.Counter(),
                              'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                              'AreaData': collections.Counter()} for i in range(time_bins)] for t in range(24)]
    l = 0
    for line in kafka_data.values():
        channel_offset = (int(
            list(kafka_data.keys())[l][11:]) - 1) * 6  # create and offset for channel based on ipaddress
        l = l + 1
        for list_line in line:
            kafka_data_length = (len(list_line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                channel = int(((bin(int('1' + (list_line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
                if channel in ch_list:
                    time_split = list_line[i + 2:i + 8]
                    time = (int(time_split[:3], 16))  # only using the top three bits for development
                    # time = (int(list_line[i + 2:i + 8], 16))
                    position[channel][time].append(int(list_line[i + 29:i + 32], 16))
                    PulseHeight[channel][time].append(int(list_line[i + 26:i + 29], 16))
                    StartSig[channel][time].append(int(list_line[i + 23:i + 26], 16))
                    Misplace[channel][time].append(int(list_line[i + 20:i + 23], 16))
                    MaxSlope[channel][time].append(int(list_line[i + 17:i + 20], 16))
                    AreaData[channel][time].append(int(list_line[i + 14:i + 17], 16))
    for i in ch_list:  # range(0, 24, 1):
        for t in range(time_bins):
            kafka_time_dict_list[i][t]['position'] = collections.Counter(position[i][t])
            kafka_time_dict_list[i][t]['PulseHeight'] = collections.Counter(PulseHeight[i][t])
            kafka_time_dict_list[i][t]['StartSig'] = collections.Counter(StartSig[i][t])
            kafka_time_dict_list[i][t]['Misplace'] = collections.Counter(Misplace[i][t])
            kafka_time_dict_list[i][t]['MaxSlope'] = collections.Counter(MaxSlope[i][t])
            kafka_time_dict_list[i][t]['AreaData'] = collections.Counter(AreaData[i][t])
    return kafka_time_dict_list


def data_split_dict_channel_ip_combine_PosTime(kafka_data, ch_list):
    time_bins = int(1024)
    time_factor = 16777215 / time_bins
    position = np.zeros((24, 256, time_bins), dtype=int)
    l = 0
    for line in kafka_data.values():
        channel_offset = (int(
            list(kafka_data.keys())[l][11:]) - 1) * 6  # create and offset for channel based on ipaddress
        l = l + 1
        for list_line in line:
            kafka_data_length = (len(list_line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                if list_line[i:i + 32] != '00000000000000000000000000000000':
                    channel = int(((bin(int('1' + (list_line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
                    if channel in ch_list:
                        time_split = int(list_line[i + 2:i + 8], 16)
                        time = int(time_split / 1000)  # set to 5000 for TS2
                        # if time_split != 0:
                        #     time = int(time_factor / time_split)
                        # else:
                        #     time = 0
                        pos = int(list_line[i + 29:i + 32], 16)
                        scaled_pos = int(pos / 16)
                        # pos = int(int(list_line[i + 29:i + 32], 16)/16)
                        position[channel, scaled_pos, time] += 1
    return position

def data_split_dict_channel_ip_combine_Pos_PulseTime(kafka_data, ch_list):
    time_bins = int(1024)
    time_factor = 16777215 / time_bins
    position = np.zeros((24, 256, time_bins), dtype=int)
    pulseheight = np.zeros((24, 256, time_bins), dtype=int)
    l = 0
    for line in kafka_data.values():
        channel_offset = (int(
            list(kafka_data.keys())[l][11:]) - 1) * 6  # create and offset for channel based on ipaddress
        l = l + 1
        for list_line in line:
            kafka_data_length = (len(list_line))
            for i in range(0, kafka_data_length, 32):  # return 32 character chunks
                if list_line[i:i + 32] != '00000000000000000000000000000000':
                    channel = int(((bin(int('1' + (list_line[i + 11:i + 13]), 16))[3:])[3:6]), 2) + channel_offset
                    if channel in ch_list:
                        time_split = int(list_line[i + 2:i + 8], 16)
                        time = int(time_split / 1000)  # set to 5000 for TS2
                        # if time_split != 0:
                        #     time = int(time_factor / time_split)
                        # else:
                        #     time = 0
                        pos = int(list_line[i + 29:i + 32], 16)
                        plh = (int(list_line[i + 26:i + 29], 16))
                        scaled_pos = int(pos / 16)
                        scaled_plh = int(plh / 16)
                        # pos = int(int(list_line[i + 29:i + 32], 16)/16)
                        position[channel, scaled_pos, time] += 1
                        pulseheight[channel, scaled_plh, time] += 1
    return position, pulseheight



###################combines two dictionaries with matching keys#####################
def dict_add(dicta, dictb):
    for key in dicta:
        if key in dictb:
            dicta[key] = dicta[key] + dictb[key]
        else:
            pass
    return dicta


def kafka_live_data_proc(*args):
    procdata = kafka_data_decoder_ip_dict(args)
    # for i in procdata:
    procdata = data_split_live_dict(procdata)
    plot_func.dict_create(procdata, procdata.get("packet_info"))  # [i]), i)
    return procdata


def live_data_test(*args):
    # print(args)
    return args


def data_split_live_dict(kafka_data):
    current_data = {'packet': "", 'packet_info': ""}
    position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData = [], [], [], [], [], []
    kafka_data_dict = {'position': collections.Counter(), 'PulseHeight': collections.Counter(),
                       'StartSig': collections.Counter(),
                       'Misplace': collections.Counter(), 'MaxSlope': collections.Counter(),
                       'AreaData': collections.Counter(), 'packet_info': ""}
    # for line in kafka_data.get("packet"):
    line = kafka_data.get("packet")
    kafka_data_length = (len(line))
    for i in range(1, kafka_data_length, 32):  # return 32 character chunks
        position.append(int(line[i + 29:i + 32], 16))
        PulseHeight.append(int(line[i + 26:i + 29], 16))
        StartSig.append(int(line[i + 23:i + 26], 16))
        Misplace.append(int(line[i + 20:i + 23], 16))
        MaxSlope.append(int(line[i + 17:i + 20], 16))
        AreaData.append(int(line[i + 14:i + 17], 16))
    kafka_data_dict['position'] = collections.Counter(position)
    kafka_data_dict['PulseHeight'] = collections.Counter(PulseHeight)
    kafka_data_dict['StartSig'] = collections.Counter(StartSig)
    kafka_data_dict['Misplace'] = collections.Counter(Misplace)
    kafka_data_dict['MaxSlope'] = collections.Counter(MaxSlope)
    kafka_data_dict['AreaData'] = collections.Counter(AreaData)
    kafka_data_dict['packet_info'] = kafka_data.get("packet_info")
    return kafka_data_dict
