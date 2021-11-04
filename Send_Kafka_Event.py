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
from datetime import datetime
from dateutil.tz import tzutc
import uuid

SERVER = "itachi.isis.cclrc.ac.uk:9092"
TOPIC = "MAPSTEST_events"
producer = Producer({"bootstrap.servers": SERVER})

def send_flatBuffer(data):
    """
    Send data to kafka
    Args:
        data: Flatbuffer Data to send to Kafka
        instrument: The instrument to send data for
    """
    producer.produce(TOPIC, data)
    producer.flush()

def _create_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": SERVER,
            "group.id": uuid.uuid4(),
        })
    return consumer

def get_data_between(start_time: datetime, end_time: datetime, instrument="MAPS"):
    """
    Get the data between the two given times.
    Note that this is based on the timestamp of when the data was put into kafka.
    Args:
        start_time: The beginning of where you want the data from
        end_time: The end of where you want the data to
        instrument: The instrument to get the data from
    """
    consumer = _create_consumer()

    epoch = datetime.fromtimestamp(0, tzutc())

    start_time = start_time.astimezone(tzutc())
    end_time = end_time.astimezone(tzutc())

    def get_part_offset(dt):
        time_since_epoch = int((dt - epoch).total_seconds() * 1000)
        return consumer.offsets_for_times([TopicPartition(TOPIC.format(instrument), 0, time_since_epoch)])[0]

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
    return [data.value() for data in consumer.consume(number_to_consume)]
