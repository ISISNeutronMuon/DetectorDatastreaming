# This file is part of the ISIS IBEX application.
# Copyright (C) 2012-2021 Science & Technology Facilities Council.
# All rights reserved.
#
# This program is distributed in the hope that it will be useful.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License v1.0 which accompanies this distribution.
# EXCEPT AS EXPRESSLY SET FORTH IN THE ECLIPSE PUBLIC LICENSE V1.0, THE PROGRAM
# AND ACCOMPANYING MATERIALS ARE PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND.  See the Eclipse Public License v1.0 for more details.
#
# You should have received a copy of the Eclipse Public License v1.0
# along with this program; if not, you can obtain a copy from
# https://www.eclipse.org/org/documents/epl-v10.php or
# http://opensource.org/licenses/eclipse-1.0.php
from confluent_kafka import Producer, Consumer, TopicPartition
from collections.abc import Callable
import threading
import json
from datetime import datetime
import uuid


SERVER = "hinata.isis.cclrc.ac.uk:9092"
TOPIC = "MAPS_detector_diagnostics"
serialiser = lambda v: json.dumps(v).encode('utf-8')
deserialiser = lambda v: json.loads(v)


def _create_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": SERVER,
            "group.id": uuid.uuid4(),
        })
    return consumer


producer = Producer({"bootstrap.servers": SERVER})


def send_data(data: dict):
    """
    Send data to kafka
    Args:
        data: A dictionary representing the data.
    """
    producer.produce(TOPIC, serialiser(data))
    producer.flush()


def do_func_on_live_data(my_func: Callable):
    """
    Passes any live data to my_func.
    Args:
        my_func: The function to call with the live data.
    """
    consumer = _create_consumer()
    consumer.subscribe([TOPIC])

    def live_data_thread():
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            my_func(json.loads(str(msg.value(), encoding='utf-8')))

    thread = threading.Thread(target=live_data_thread)
    thread.start()


def get_data_between(start_time: datetime, end_time: datetime):
    """
    Get the data between the two given times.
    Note that this is based on the timestamp of when the data was put into kafka.
    Args:
        start_time: The beginning of where you want the data from
        end_time: The end of where you want the data to
    """
    consumer = _create_consumer()

    epoch = datetime.utcfromtimestamp(0)

    def get_part_offset(dt):
        time_since_epoch = int((dt - epoch).total_seconds() * 1000)
        return consumer.offsets_for_times([TopicPartition(TOPIC, 0, time_since_epoch)])[0]

    try:
        start_time_part_offset = get_part_offset(start_time)
        end_time_part_offset = get_part_offset(end_time)
    except Exception:
        # Sometimes the consumer isn't quite ready, try once more
        start_time_part_offset = get_part_offset(start_time)
        end_time_part_offset = get_part_offset(end_time)

    offsets = [start_time_part_offset.offset, end_time_part_offset.offset]

    if offsets[0] == -1:
        print("No data found for time period")
        return

    consumer.assign([start_time_part_offset])

    if offsets[1] == -1:
        offsets[1] = consumer.get_watermark_offsets(end_time_part_offset)[1]

    number_to_consume = offsets[1] - offsets[0]

    return [json.loads(str(data.value(), encoding="utf-8"))
            for data in consumer.consume(number_to_consume)]


