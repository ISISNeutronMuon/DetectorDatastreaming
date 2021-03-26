import matplotlib.pyplot as plt


def plt_position(graph_list, i):
    if sum(graph_list) > 3:
        if i < 3:
            axes_col = i
            axes_row = 0
        else:
            axes_col = i - 3
            axes_row = 1
        axes = axes_row, axes_col
    else:
        axes = i
    return axes


# ######### plot with choice of 32 channels, graph types on same plots ###########
def dict_sel_ch_grph_same(dict_data, ch_list, graph_list):
    if sum(graph_list) > 3:
        number_of_cols = 3
        number_of_rows = 2
    else:
        number_of_cols = sum(graph_list)
        number_of_rows = 1
    axes_count = 0
    figure, axes = plt.subplots(nrows=number_of_rows, ncols=number_of_cols)
    if sum(graph_list) == 1:
        for i in ch_list:
            if len(dict_data[i]['position']):
                if graph_list[0] == 1:
                    axes.bar(list(dict_data[i]['position'].keys()), dict_data[i]['position'].values(),
                             label="CH" + str(i))
                    axes.set_title('Position')
                elif graph_list[1] == 1:
                    axes.bar(list(dict_data[i]['PulseHeight'].keys()), dict_data[i]['PulseHeight'].values(),
                             label="CH" + str(i))
                    axes.set_title('Pulse Height')
                elif graph_list[2] == 1:
                    axes.bar(list(dict_data[i]['StartSig'].keys()), dict_data[i]['StartSig'].values(),
                             label="CH" + str(i))
                    axes.set_title('Start Sig')
                elif graph_list[3] == 1:
                    axes.bar(list(dict_data[i]['Misplace'].keys()), dict_data[i]['Misplace'].values(),
                             label="CH" + str(i))
                    axes.set_title('Misplace')
                elif graph_list[4] == 1:
                    axes.bar(list(dict_data[i]['MaxSlope'].keys()), dict_data[i]['MaxSlope'].values(),
                             label="CH" + str(i))
                    axes.set_title('Max Slope')
                elif graph_list[5] == 1:
                    axes.bar(list(dict_data[i]['AreaData'].keys()), dict_data[i]['AreaData'].values(),
                             label="CH" + str(i))
                    axes.set_title('Area Data')
        axes.legend()
    else:
        for i in ch_list:
            if len(dict_data[i]['position']):
                if graph_list[0] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['position'].keys()),
                                                                   dict_data[i]['position'].values(),
                                                                   label="CH" + str(i))
                    axes[plt_position(graph_list, axes_count)].set_title('Position')
                    axes_count = axes_count + 1
                if graph_list[1] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['PulseHeight'].keys()),
                                                                   dict_data[i]['PulseHeight'].values())
                    axes[plt_position(graph_list, axes_count)].set_title('Pulse Height')
                    axes_count = axes_count + 1
                if graph_list[2] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['StartSig'].keys()),
                                                                   dict_data[i]['StartSig'].values())
                    axes[plt_position(graph_list, axes_count)].set_title('Start Sig')
                    axes_count = axes_count + 1
                if graph_list[3] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['Misplace'].keys()),
                                                                   dict_data[i]['Misplace'].values())
                    axes[plt_position(graph_list, axes_count)].set_title('Misplace')
                    axes_count = axes_count + 1
                if graph_list[4] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['MaxSlope'].keys()),
                                                                   dict_data[i]['MaxSlope'].values())
                    axes[plt_position(graph_list, axes_count)].set_title('Max Slope')
                    axes_count = axes_count + 1
                if graph_list[5] == 1:
                    axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['AreaData'].keys()),
                                                                   dict_data[i]['AreaData'].values())
                    axes[plt_position(graph_list, axes_count)].set_title('Area Data')
                axes_count = 0
        axes[plt_position(graph_list, axes_count)].legend()
    return


# ######### plot with choice of 32 channels, graph types on separate plot ############
def dict_sel_ch_grph_separ(dict_data, ch_list, graph_list):
    figure, axes = plt.subplots(nrows=len(ch_list), ncols=sum(graph_list))
    axes_row = 0
    axes_col = 0
    if len(ch_list) == 1 and sum(graph_list) == 1:
        for i in ch_list:
            if len(dict_data[i]['position']):
                axes.set_ylabel("CH" + str(i))
                if graph_list[0] == 1:
                    axes.bar(list(dict_data[i]['position'].keys()), dict_data[i]['position'].values())
                elif graph_list[1] == 1:
                    axes.bar(list(dict_data[i]['PulseHeight'].keys()), dict_data[i]['PulseHeight'].values())
                elif graph_list[2] == 1:
                    axes.bar(list(dict_data[i]['StartSig'].keys()), dict_data[i]['StartSig'].values())
                if graph_list[3] == 1:
                    axes.bar(list(dict_data[i]['Misplace'].keys()), dict_data[i]['Misplace'].values())
                if graph_list[4] == 1:
                    axes.bar(list(dict_data[i]['MaxSlope'].keys()), dict_data[i]['MaxSlope'].values())
                if graph_list[5] == 1:
                    axes.bar(list(dict_data[i]['AreaData'].keys()), dict_data[i]['AreaData'].values())
        if graph_list[0] == 1:
            axes.set_title('Position')
        elif graph_list[1] == 1:
            axes.set_title('Pulse Height')
        elif graph_list[2] == 1:
            axes.set_title('Start Sig')
        elif graph_list[3] == 1:
            axes.set_title('Misplace')
        elif graph_list[4] == 1:
            axes.set_title('Max Slope')
        elif graph_list[5] == 1:
            axes.set_title('Area Data')
    elif len(ch_list) == 1 or sum(graph_list) == 1:
        for i in ch_list:
            if len(dict_data[i]['position']):
                axes[axes_col].set_ylabel("CH" + str(i))
                if graph_list[0] == 1:
                    axes[axes_col].bar(list(dict_data[i]['position'].keys()), dict_data[i]['position'].values())
                    axes_col = axes_col + 1
                if graph_list[1] == 1:
                    axes[axes_col].bar(list(dict_data[i]['PulseHeight'].keys()), dict_data[i]['PulseHeight'].values())
                    axes_col = axes_col + 1
                if graph_list[2] == 1:
                    axes[axes_col].bar(list(dict_data[i]['StartSig'].keys()), dict_data[i]['StartSig'].values())
                    axes_col = axes_col + 1
                if graph_list[3] == 1:
                    axes[axes_col].bar(list(dict_data[i]['Misplace'].keys()), dict_data[i]['Misplace'].values())
                    axes_col = axes_col + 1
                if graph_list[4] == 1:
                    axes[axes_col].bar(list(dict_data[i]['MaxSlope'].keys()), dict_data[i]['MaxSlope'].values())
                    axes_col = axes_col + 1
                if graph_list[5] == 1:
                    axes[axes_col].bar(list(dict_data[i]['AreaData'].keys()), dict_data[i]['AreaData'].values())
                    axes_col = axes_col + 1
        axes_col = 0
        if graph_list[0] == 1:
            axes[axes_col].set_title('Position')
            axes_col = axes_col + 1
        if graph_list[1] == 1:
            axes[axes_col].set_title('Pulse Height')
            axes_col = axes_col + 1
        if graph_list[2] == 1:
            axes[axes_col].set_title('Start Sig')
            axes_col = axes_col + 1
        if graph_list[3] == 1:
            axes[axes_col].set_title('Misplace')
            axes_col = axes_col + 1
        if graph_list[4] == 1:
            axes[axes_col].set_title('Max Slope')
            axes_col = axes_col + 1
        if graph_list[5] == 1:
            axes[axes_col].set_title('Area Data')
    else:
        for i in ch_list:
            if len(dict_data[i]['position']):
                axes[axes_row, 0].set_ylabel("CH" + str(i))
                if graph_list[0] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['position'].keys()),
                                                 dict_data[i]['position'].values())
                    axes_col = axes_col + 1
                if graph_list[1] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['PulseHeight'].keys()),
                                                 dict_data[i]['PulseHeight'].values())
                    axes_col = axes_col + 1
                if graph_list[2] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['StartSig'].keys()),
                                                 dict_data[i]['StartSig'].values())
                    axes_col = axes_col + 1
                if graph_list[3] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['Misplace'].keys()),
                                                 dict_data[i]['Misplace'].values())
                    axes_col = axes_col + 1
                if graph_list[4] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['MaxSlope'].keys()),
                                                 dict_data[i]['MaxSlope'].values())
                    axes_col = axes_col + 1
                if graph_list[5] == 1:
                    axes[axes_row, axes_col].bar(list(dict_data[i]['AreaData'].keys()),
                                                 dict_data[i]['AreaData'].values())
                    axes_col = axes_col + 1
            axes_row = axes_row + 1
            axes_col = 0
        if graph_list[0] == 1:
            axes[0, axes_col].set_title('Position')
            axes_col = axes_col + 1
        if graph_list[1] == 1:
            axes[0, axes_col].set_title('Pulse Height')
            axes_col = axes_col + 1
        if graph_list[2] == 1:
            axes[0, axes_col].set_title('Start Sig')
            axes_col = axes_col + 1
        if graph_list[3] == 1:
            axes[0, axes_col].set_title('Misplace')
            axes_col = axes_col + 1
        if graph_list[4] == 1:
            axes[0, axes_col].set_title('Max Slope')
            axes_col = axes_col + 1
        if graph_list[5] == 1:
            axes[0, axes_col].set_title('Area Data')
            axes_col = axes_col + 1
    # axes[0, 0].legend()
    return


def data_plot_string(kafka_data):
    position, PulseHeight, StartSig, Misplace, MaxSlope, AreaData, Time = [], [], [], [], [], [], []
    position_combined, PulseHeight_combined, StartSig_combined, Misplace_combined, MaxSlope_combined, AreaData_combined, Time_combined = [], [], [], [], [], [], []
    kafka_data = kafka_data.splitlines()  # turn carriage return string into a list
    kafka_data = [e[128:] for e in kafka_data]  # remove the header from the list
    for line in kafka_data:
        position = [(int(line[i + 29:i + 32], 16) / 8) for i in range(0, len(line), 32)]
        PulseHeight = [(int(line[i + 26:i + 29], 16) / 8) for i in range(0, len(line), 32)]
        StartSig = [(int(line[i + 23:i + 26], 16) / 8) for i in range(0, len(line), 32)]
        Misplace = [(int(line[i + 20:i + 23], 16) / 8) for i in range(0, len(line), 32)]
        MaxSlope = [(int(line[i + 17:i + 20], 16) / 8) for i in range(0, len(line), 32)]
        AreaData = [(int(line[i + 14:i + 17], 16) / 8) for i in range(0, len(line), 32)]
        Time = [(int(line[i + 3:i + 8], 16)) for i in range(0, len(line), 32)]
        position_combined = position_combined + position
        PulseHeight_combined = PulseHeight_combined + PulseHeight
        StartSig_combined = StartSig_combined + StartSig
        Misplace_combined = Misplace_combined + Misplace
        MaxSlope_combined = MaxSlope_combined + MaxSlope
        AreaData_combined = AreaData_combined + AreaData
        Time_combined = Time_combined + Time
    figure, axes = plt.subplots(nrows=2, ncols=4)
    axes[0, 0].hist(position_combined, bins=255)
    axes[0, 0].set_title('Position')
    axes[0, 1].hist(PulseHeight_combined, bins=255)
    axes[0, 1].set_title('Pulse Height')
    axes[0, 2].hist(StartSig_combined, bins=255)
    axes[0, 2].set_title('Start Sig')
    axes[1, 0].hist(Misplace_combined, bins=255)
    axes[1, 0].set_title('Misplace')
    axes[1, 1].hist(MaxSlope_combined, bins=255)
    axes[1, 1].set_title('Max Slope')
    axes[1, 2].hist(AreaData_combined, bins=255)
    axes[1, 2].set_title('Area Data')
    axes[0, 3].hist(Time_combined)
    axes[0, 3].set_title('Time')
    plt.show()
    return

    #######Creates figure from the input data################


def dict_create(dict_data, title):
    figure, axes = plt.subplots(nrows=2, ncols=3)
    figure.canvas.set_window_title(title)
    axes[0, 0].bar(list(dict_data['position'].keys()), dict_data['position'].values())
    axes[0, 0].set_title('Position')
    axes[0, 1].bar(list(dict_data['PulseHeight'].keys()), dict_data['PulseHeight'].values())
    axes[0, 1].set_title('Pulse Height')
    axes[0, 2].bar(list(dict_data['StartSig'].keys()), dict_data['StartSig'].values())
    axes[0, 2].set_title('Start Sig')
    axes[1, 0].bar(list(dict_data['Misplace'].keys()), dict_data['Misplace'].values())
    axes[1, 0].set_title('Misplace')
    axes[1, 1].bar(list(dict_data['MaxSlope'].keys()), dict_data['MaxSlope'].values())
    axes[1, 1].set_title('Max Slope')
    axes[1, 2].bar(list(dict_data['AreaData'].keys()), dict_data['AreaData'].values())
    axes[1, 2].set_title('Area Data')
    return

# ###############################################       OLD     ################################################
# ######### plot channels in list on same graph and allows you to choose which graphs to display-6 channels############
# def plt_dict_sel_ch_graph(dict_data, title, ch_list, graph_list):
#     number_of_cols = (math.ceil(sum(graph_list) / 2) + 1)
#     if sum(graph_list) > 3:
#         number_of_cols = 3
#         number_of_rows = 2
#     else:
#         number_of_rows = 1
#     axes_count = 0
#     figure, axes = plt.subplots(nrows=number_of_rows, ncols=number_of_cols)
#     figure.canvas.set_window_title(title)
#     bar_colour = ['black', 'brown', 'red', 'orange', 'yellow', 'green']
#     for i in ch_list:
#         if len(dict_data[i]['position']):
#             if graph_list[0] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['position'].keys()),
#                                                                dict_data[i]['position'].values(),
#                                                                color=bar_colour[i], label="CH" + str(i))
#                 axes[plt_position(graph_list, axes_count)].set_title('Position')
#                 axes_count = axes_count + 1
#             if graph_list[1] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['PulseHeight'].keys()),
#                                                                dict_data[i]['PulseHeight'].values(),
#                                                                color=bar_colour[i])
#                 axes[plt_position(graph_list, axes_count)].set_title('Pulse Height')
#                 axes_count = axes_count + 1
#             if graph_list[2] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['StartSig'].keys()),
#                                                                dict_data[i]['StartSig'].values(),
#                                                                color=bar_colour[i])
#                 axes[plt_position(graph_list, axes_count)].set_title('Start Sig')
#                 axes_count = axes_count + 1
#             if graph_list[3] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['Misplace'].keys()),
#                                                                dict_data[i]['Misplace'].values(),
#                                                                color=bar_colour[i])
#                 axes[plt_position(graph_list, axes_count)].set_title('Misplace')
#                 axes_count = axes_count + 1
#             if graph_list[4] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['MaxSlope'].keys()),
#                                                                dict_data[i]['MaxSlope'].values(),
#                                                                color=bar_colour[i])
#                 axes[plt_position(graph_list, axes_count)].set_title('Max Slope')
#                 axes_count = axes_count + 1
#             if graph_list[5] == 1:
#                 axes[plt_position(graph_list, axes_count)].bar(list(dict_data[i]['AreaData'].keys()),
#                                                                dict_data[i]['AreaData'].values(),
#                                                                color=bar_colour[i])
#                 axes[plt_position(graph_list, axes_count)].set_title('Area Data')
#             axes_count = 0
#     axes[plt_position(graph_list, axes_count)].legend()
#     return

# # plot all channels on same graph with channel choice
# def plt_dict_sel_ch(dict_data, title, ch_list):
#     figure, axes = plt.subplots(nrows=2, ncols=3)
#     figure.canvas.set_window_title(title)
#     bar_colour = ['black', 'brown', 'red', 'orange', 'yellow', 'green']
#     for i in ch_list:
#         if len(dict_data[i]['position']):
#             axes[0, 0].bar(list(dict_data[i]['position'].keys()), dict_data[i]['position'].values(),
#                            color=bar_colour[i], label="CH" + str(i))
#             axes[0, 1].bar(list(dict_data[i]['PulseHeight'].keys()), dict_data[i]['PulseHeight'].values(),
#                            color=bar_colour[i])
#             axes[0, 2].bar(list(dict_data[i]['StartSig'].keys()), dict_data[i]['StartSig'].values(),
#                            color=bar_colour[i])
#             axes[1, 0].bar(list(dict_data[i]['Misplace'].keys()), dict_data[i]['Misplace'].values(),
#                            color=bar_colour[i])
#             axes[1, 1].bar(list(dict_data[i]['MaxSlope'].keys()), dict_data[i]['MaxSlope'].values(),
#                            color=bar_colour[i])
#             axes[1, 2].bar(list(dict_data[i]['AreaData'].keys()), dict_data[i]['AreaData'].values(),
#                            color=bar_colour[i])
#     axes[0, 0].legend()
#     axes[0, 0].set_title('Position')
#     axes[0, 1].set_title('Pulse Height')
#     axes[0, 2].set_title('Start Sig')
#     axes[1, 0].set_title('Misplace')
#     axes[1, 1].set_title('Max Slope')
#     axes[1, 2].set_title('Area Data')
#     return

# # plot all channels on same graph
# def plt_dict_all_ch(dict_data, title):
#     figure, axes = plt.subplots(nrows=2, ncols=3, figsize=(8, 6))
#     figure.canvas.set_window_title(title)
#     bar_colour = ['black', 'brown', 'red', 'orange', 'yellow', 'green']
#     for i in range(0, 6, 1):
#         if len(dict_data[i]['position']):
#             axes[0, 0].bar(list(dict_data[i]['position'].keys()), dict_data[i]['position'].values(),
#                            color=bar_colour[i], label="CH" + str(i))
#             axes[0, 1].bar(list(dict_data[i]['PulseHeight'].keys()), dict_data[i]['PulseHeight'].values(),
#                            color=bar_colour[i])
#             axes[0, 2].bar(list(dict_data[i]['StartSig'].keys()), dict_data[i]['StartSig'].values(),
#                            color=bar_colour[i])
#             axes[1, 0].bar(list(dict_data[i]['Misplace'].keys()), dict_data[i]['Misplace'].values(),
#                            color=bar_colour[i])
#             axes[1, 1].bar(list(dict_data[i]['MaxSlope'].keys()), dict_data[i]['MaxSlope'].values(),
#                            color=bar_colour[i])
#             axes[1, 2].bar(list(dict_data[i]['AreaData'].keys()), dict_data[i]['AreaData'].values(),
#                            color=bar_colour[i])
#     axes[0, 0].set_title('Position')
#     axes[0, 0].legend()
#     axes[0, 1].set_title('Pulse Height')
#     axes[0, 2].set_title('Start Sig')
#     axes[1, 0].set_title('Misplace')
#     axes[1, 1].set_title('Max Slope')
#     axes[1, 2].set_title('Area Data')
#     return
