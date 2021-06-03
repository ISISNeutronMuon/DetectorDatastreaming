# from datetime import datetime
import datetime
import pandas as pd
import data_proc_func
import argparse

csv_page = []
# parser = argparse.ArgumentParser()
# parser.add_argument('--ch_list', nargs='+', type=int)
# parser.add_argument('--all_channels', nargs='+', type=bool)
# args = parser.parse_args()
# if args.all_channels:
#     ch_list = list(range(0,24,1))
# else:
#     ch_list = tuple(args.ch_list)
# ch_list = [0, 4]
ch_list = list(range(0,24,1))
# ######################code to select historic data to process###################################
start_time = datetime.datetime(year=2021, month=5, day=14, hour=0, minute=00)
# end_time = datetime.datetime(year=2021, month=4, day=6, hour=12, minute=10)
# start_time = (datetime.datetime.now() - datetime.timedelta(days=1))
end_time = datetime.datetime.now()
procdata = data_proc_func.kafka_frame_decoder_ip_dict_split_line(start_time, end_time)
csv_data = data_proc_func.data_split_dict_channel_ip_combine_PosTime(procdata, ch_list)

writer2 = pd.ExcelWriter('mult_sheets_PosTime.xlsx')
for i in ch_list:
    print("adding channel " + str(i))
    csv_page = pd.DataFrame(csv_data[i]).sort_index()
    csv_page.to_excel(writer2, sheet_name=str(i), index=True)
print("Saving Spreadsheet")
writer2.save()
print('done')
exit()
