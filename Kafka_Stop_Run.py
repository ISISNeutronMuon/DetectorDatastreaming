import Send_Kafka_RunInfo                                       # import kafka run info sender lib
from streaming_data_types.run_stop_6s4t import serialise_6s4t  # import ESS Flatbuffer serialiser for 6s4t
import time                                                     # import time to get current time for runstart

# Define all values to send to Kafka - as data type "set" to send all data to serialiser in one package
RunInfo = {
    "job_id": "1",
    "stop_time": int(time.time() * 1000),
    "run_name": "test_run",
    "service_id": "IESGPythonTesting",
    "command_id": "PythonStopTestRun",
}

# Serialise and send set to kafka:
print("Kafka stop streaming test run, info: ")
print(RunInfo)
Serialised_6S4T = serialise_6s4t(**RunInfo)               # Serialise the info set into the 6s4t data set
print("Serialised data: ", Serialised_6S4T)
Send_Kafka_RunInfo.send_flatBuffer(Serialised_6S4T)     # Send the serialised data to the kafka run info topic
print("Sent to Kafka, test run stopped.")