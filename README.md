# Detector Datastreaming
Software for streaming data from detectors and analysing it 

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

my_data = get_data_between(datetime.now(tzutc()) - timedelta(hours=1), datetime.now(tzutc()))
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
