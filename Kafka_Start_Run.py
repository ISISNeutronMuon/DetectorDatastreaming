import Send_Kafka_RunInfo                                       # import kafka run info sender lib
from streaming_data_types.run_start_pl72 import serialise_pl72  # import ESS Flatbuffer serialiser for PL72
import time                                                     # import time to get current time for runstart

# Define all values to send to Kafka - as set data type to send all data to serialiser in one package
RunInfo = {
    "job_id": "1",
    "filename": "MAPS_DAES_IESG_Testing.nxs",
    "start_time": (time.time() * 1000),
    "stop_time": 0,
    "run_name": "test_run",
    "nexus_structure": "{}",
    "service_id": "IESGPythonTesting",
    "instrument_name": "MAPS",
    "broker": "itachi.isis.cclrc.ac.uk:9092",
    "metadata": None,
    "detector_spectrum_map": None,
    "control_topic": "{}MAPSTEST_runInfo",
}

# Serialise and send set to kafka:
Serialised_PL72 = serialise_pl72(RunInfo)               # Serialise the info set into the pl72 data set
Send_Kafka_RunInfo.send_flatBuffer(Serialised_PL72)     # Send the serialised data to the kafka run info topic