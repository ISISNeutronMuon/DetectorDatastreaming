import Send_Kafka_RunInfo  # import kafka run info sender lib
from streaming_data_types.run_start_pl72 import serialise_pl72  # import ESS Flatbuffer serialiser for PL72
import time  # import time to get current time for runstart

# in development JSON for MAPS
nexus_structure_json_file = "MAPs_Nexus_Structure.json"

# Demo Structure from Github ESS folder.
# nexus_structure_json_file = "test_nexus.json"

starttime = int(time.time() * 1_000)

# grab Nexus Structure from file
with open(nexus_structure_json_file, "r") as nexus_structure_json:
    Nexus_Structure = nexus_structure_json.read()

# file to save name
file_name = "MAPSTEST_" + str(starttime) + ".nxs"

# Define all values to send to Kafka - as set data type to send all data to serialiser in one package
RunInfo = {
    "job_id": "1",
    "filename": file_name,
    "start_time": starttime,
    "stop_time": None,
    "run_name": "wsOut",
    "nexus_structure": Nexus_Structure,
    "service_id": "IESGPythonTesting",
    "instrument_name": "MAPS",
    "broker": "livedata.isis.cclrc.ac.uk:9092",
    # "metadata": None,
    "control_topic": "MAPSTEST2_runInfo",
}

# Serialise and send set to kafka:
print("Kafka start streaming test run, info: ")
print(RunInfo)
Serialised_PL72 = serialise_pl72(**RunInfo)  # Serialise the info set into the pl72 data set
print("Serialised data: ", Serialised_PL72)
Send_Kafka_RunInfo.send_flatBuffer(Serialised_PL72)  # Send the serialised data to the kafka run info topic
print("Sent to Kafka, test run started.")
