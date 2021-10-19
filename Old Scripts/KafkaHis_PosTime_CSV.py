# from datetime import datetime
import datetime
import pandas as pd
import data_proc_func
import numpy
import argparse

csv_page = []

# ch_list = [0]
ch_list = list(range(0,24,1))
# ######################code to select historic data to process###################################
# start_time = datetime.datetime(year=2021, month=5, day=14, hour=0, minute=00)
# end_time = datetime.datetime(year=2021, month=4, day=6, hour=12, minute=10)
start_time = (datetime.datetime.now() - datetime.timedelta(minutes=65))
end_time = datetime.datetime.now()
procdata = data_proc_func.kafka_frame_decoder_ip_dict_split_line(start_time, end_time)
csv_data_pos, csv_data_pulse = data_proc_func.data_split_dict_channel_ip_combine_Pos_PulseTime(procdata, ch_list)
# csv_data = data_proc_func.data_split_dict_channel_ip_combine_PosTime(procdata, ch_list)

#writer2 = pd.ExcelWriter('mult_sheets_PosTime.xlsx')
for i in ch_list:
    numpy.savetxt(str(i)+"pos.csv", csv_data_pos[i], delimiter=",", fmt='%d')
    numpy.savetxt(str(i)+"puls.csv", csv_data_pulse[i], delimiter=",", fmt='%d')
    print("adding chanel " + str(i))
print('done')
exit()