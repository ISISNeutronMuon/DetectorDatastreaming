import Send_Kafka_Event
import datetime
from streaming_data_types.eventdata_ev42 import deserialise_ev42 # import ESS Flatbuffer serialiser

start_time = (datetime.datetime.now() - datetime.timedelta(minutes=15))
end_time = datetime.datetime.now()
print("Getting Data: ")
KafkaEvents = Send_Kafka_Event.get_data_between(start_time, end_time)
print(KafkaEvents)

for i in KafkaEvents:
    RawEvent = deserialise_ev42(KafkaEvents[i])
    print(RawEvent)