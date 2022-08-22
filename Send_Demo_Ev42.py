import time

import ADC_Data_Processor
import Send_Kafka_Event
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000000000

for i in range(100000):
    nsSinceEpoch = int(unix_time_millis(datetime.datetime.now()))
    streamIP = "192.168.2.101"
    messageID = i
    pulseTime = int(time.time() * 1_000_000)
    TOF = []
    DetectorID = []
    for test in range(10):
        for d in range(255):
            DetectorID.append(d+11101001)
            TOF.append(d)

    EV42_FrameData = ADC_Data_Processor.Serialise_EV42(streamIP, messageID, pulseTime, TOF, DetectorID)
    Send_Kafka_Event.send_flatBuffer(EV42_FrameData)
    print("Sent Data ", i,"time: ", pulseTime, ", TOF: ", TOF, "Detector ID:", DetectorID)

    time.sleep(1)