# Detector Datastreaming
This is a collection of python scripts for allowing the IESG to stream ISIS Neutron Data into mantid. There are multiple different scripts with different uses, listed below:
- ADC_Data_Processor.py - Used to process the raw ADC packets into event data in the EV42 Flatbuffer schema
- Kafka_helper.py - user to send data to Kafka
- Mutliple_MADC_DAEStreams_to_Kafka.py - Streams multiple MADCs to Kafka
- Kafka_Start_Run.py - Used to generate a start point within the Kafka topic - testing use only
- Kafka_Stop_Run.py - Used to generate a stop point within the Kafka topic - testing use only

There are multiple files used by the code that are used to configure it, for different ISIS instruments
- MADC_Boards.csv - Reads in all streaming boards within the system
- DAES_WiringTable_Instrument.csv - tells the data processor how to map ADC channels to Mantid detector arrays

## How to use the Kafka helper

### Sending Data

```python
from kafka_helper import send_data

send_data({"event": "neutron", "position": "left"}) 
send_data({"event": "neutron", "position": "right"}) 
```

### Getting Historic Data

```python
from kafka_helper import get_data_between
from datetime import datetime, timedelta
from dateutil.tz import tzutc

my_data = get_data_between(datetime.now() - timedelta(hours=1), datetime.now())
print(my_data) 
```

assuming that [sending data](#sending-data) was done in the last hour, this would print:
```python
[{"event": "neutron", "position": "left"}, {"event": "neutron", "position": "right"}]
```

### Getting Live Data
```python
from kafka_helper import send_data, do_func_on_live_data

def function_that_will_do_something_with_data(the_data):
    print(the_data)

do_func_on_live_data(function_that_will_do_something_with_data)

send_data({"event": "neutron", "position": "right"}) 
```

this will print the following:

```python
[{"event": "neutron", "position": "right"}]
```

and if left running will subsequently print every time data is sent by this or any other process.

This can be extended using matplotlib to do graphing such as:

```python
import kafka_helper
import matplotlib.pyplot as plt

histogrammed_data = {"left": 0, "right": 0}

def received_new_data(new_data):
    histogrammed_data[new_data["position"]] += 1

kafka_helper.do_func_on_live_data(received_new_data)

figure, axes = plt.subplots()
plt.ion()
plt.show()

while True:
    plt.pause(1)
    axes.bar(list(histogrammed_data.keys()), histogrammed_data.values())
    plt.draw()
```
