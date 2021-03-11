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