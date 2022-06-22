import ADC_Data_Processor
import Send_Kafka_Event
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000000000

for i in range(10):
    nsSinceEpoch = int(unix_time_millis(datetime.datetime.now()))
    streamIP = "192.168.2.101"
    messageID = 1
    pulseTime = nsSinceEpoch
    TOF = []
    DetectorID = []


    for d in range(255):
        DetectorID.append(11101001)
        TOF.append(int(d*100+10_000))

    EV42_FrameData = ADC_Data_Processor.Serialise_EV42(streamIP, messageID, pulseTime, TOF, DetectorID)
    Send_Kafka_Event.send_flatBuffer(EV42_FrameData)
    print("Sent Data ", i, ", TOF: ", TOF, "Detector ID:", DetectorID, "time: ", pulseTime)