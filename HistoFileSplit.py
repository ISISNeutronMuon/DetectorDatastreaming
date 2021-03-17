import matplotlib.pyplot as plt

raw_stream = "I:/LabVIEW/DJT/ADC_data_analysis/data/202_thread.txt"
reformatted_stream = "I:/LabVIEW/DJT/ADC_data_analysis/data/204_thread_formatted_e0.txt"
OLD_FILE0 = open(raw_stream, "r+")
NEW_FILE0 = open(reformatted_stream, "w+")
HEADER_STRING = "ffffffffffffffff0"
END_HEADER = "efffffffffffffff0"
position_combined = []
PulseHeight_combined = []
StartSig_combined = []
Misplace_combined = []
MaxSlope_combined = []
AreaData_combined = []
Time_combined = []
while True:
    # Get next line from file
    current_line = OLD_FILE0.readline().rstrip()
    # If line is empty then end of file reached
    if not current_line:
        break
    current_line = current_line.replace(HEADER_STRING, HEADER_STRING) \
        .replace(END_HEADER, END_HEADER)  # sort data to start with frame header
    current_line = current_line[128:]  # this removes the headers
    # current_line ="\n" + current_line[128:]  #this removes the headers
    position = [(int(current_line[i + 29:i + 32], 16) / 8) for i in range(0, len(current_line), 32)]
    PulseHeight = [(int(current_line[i + 26:i + 29], 16) / 8) for i in range(0, len(current_line), 32)]
    StartSig = [(int(current_line[i + 23:i + 26], 16) / 8) for i in range(0, len(current_line), 32)]
    Misplace = [(int(current_line[i + 20:i + 23], 16) / 8) for i in range(0, len(current_line), 32)]
    MaxSlope = [(int(current_line[i + 17:i + 20], 16) / 8) for i in range(0, len(current_line), 32)]
    AreaData = [(int(current_line[i + 14:i + 17], 16) / 8) for i in range(0, len(current_line), 32)]
    Time = [(int(current_line[i + 3:i + 8], 16)) for i in range(0, len(current_line), 32)]

    position_combined = position_combined + position
    PulseHeight_combined = PulseHeight_combined + PulseHeight
    StartSig_combined = StartSig_combined + StartSig
    Misplace_combined = Misplace_combined + Misplace
    MaxSlope_combined = MaxSlope_combined + MaxSlope
    AreaData_combined = AreaData_combined + AreaData
    Time_combined = Time_combined + Time

    # NEW_FILE0.write(str(Time))
    # NEW_FILE0.write(str(current_line))
# Close files

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
OLD_FILE0.close()
NEW_FILE0.close()
# return
