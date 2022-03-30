import Send_Kafka_Event
import datetime
from streaming_data_types.eventdata_ev42 import deserialise_ev42 # import ESS Flatbuffer serialiser

start_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
end_time = datetime.datetime.now()
print("Getting Data: ")
KafkaEvents = Send_Kafka_Event.get_data_between(start_time, end_time)
# print(KafkaEvents)

for i in KafkaEvents:
    RawEvent = deserialise_ev42(i)
    print(f"pulse time: {RawEvent.pulse_time} detID: {RawEvent.detector_id}")

print("Kafka Event Len:",len(KafkaEvents))
