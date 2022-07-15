
import Send_Kafka_RunInfo
import datetime
from streaming_data_types.run_start_pl72 import deserialise_pl72
from streaming_data_types.utils import get_schema

mins = 7

start_time = (datetime.datetime.now() - datetime.timedelta(days=mins))
end_time = datetime.datetime.now()
print("Getting Data: ")
RunControl_Packets = Send_Kafka_RunInfo.get_data_between(start_time, end_time)

print(start_time)
print(end_time)

file = open("run_start_messgaes.txt", "w")
file_start_ID = 0
#print(RunControl_Packets)

if RunControl_Packets is not None:
    for Packet in RunControl_Packets:
        schema = get_schema(Packet)
        if schema == "pl72":
            packet_data = deserialise_pl72(Packet)
            print(packet_data)
            file.write("Run Start - ")
            file.write(str(file_start_ID))
            file.write('\n')

            file.write("nexus structure: ")
            file.write(packet_data.nexus_structure)
            file.write('\n')
            file.write("Spec map: ")
            file.write(str(packet_data.detector_spectrum_map))
            file.write('\n')
            file.write('\n')
            file_start_ID += 1
        else:
            print("Packet not run start : ", schema)
else:
    print("No data")