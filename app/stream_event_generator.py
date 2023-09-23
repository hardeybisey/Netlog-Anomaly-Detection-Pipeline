#!/usr/bin/env python3

import os
import time
import json
import random
import argparse
from faker import Faker
from typing import NamedTuple
from datetime import datetime, timedelta
import multiprocessing
from google.cloud import pubsub_v1


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TCP_FLAGS = ["URG", "ACK", "PSH", "RST", "SYN", "FIN"]
PROTOCOL_NAMES = ["TCP", "UDP", "HTTP", "HTTPS"]
PROTOCOL_MAPPING = {"TCP": 6, "UDP": 17, "HTTP": 80, "HTTPS": 443}
project_id = os.environ.get("PROJECT_ID")
topic_name = os.environ.get("TOPIC_NAME")
nusers = int(os.environ.get("NUSERS"))
num_dest_ip = int(os.environ.get("NUM_DEST_IP"))
max_request_bytes = int(os.environ.get("MAX_REQUEST_BYTES"))
avg_requests_secs = int(os.environ.get("AVG_REQUESTS_SECS"))
min_session_events = int(os.environ.get("MIN_SESSION_EVENTS"))
max_session_events = int(os.environ.get("MAX_SESSION_EVENTS"))
    
class User(NamedTuple):
    subcriber_id: str
    src_ip: str
    src_port: int

class Destination(NamedTuple):
    dest_ip: str
    dest_port: int
    protocol_name: str

class NetLogRaw(NamedTuple):
    subscriber_id: str
    src_ip: str
    src_port: int
    dest_ip: str
    dst_port: int
    tx_bytes: int
    rx_bytes: int
    tcp_flag: int
    start_time: str
    end_time: str
    protocol_name: str
    protocol_num: int

def generate_users(nusers):
    return [User(faker.uuid4(), faker.ipv4(), faker.port_number()) for _ in range(nusers)]

def generate_destination_ips(num_dest_ip):
    protocol_name = random.choice(PROTOCOL_NAMES)
    protocol_num = PROTOCOL_MAPPING[protocol_name]
    return [Destination(faker.ipv4_private(), faker.port_number(), protocol_name) for _ in range(num_dest_ip)]

def get_user_destination_pair(users, dest):
    return list((user,random.choice(dest)) for user in users)

def normalized_bytes(lag_time):
    random_byte = random.uniform(10, 100)
    normalized_byte = random_byte * lag_time
    return int(min(normalized_byte, max_request_bytes))

def generate_event(start_time, user, dest):
    time_diff = random.uniform(0, avg_requests_secs * 2)
    end_time = start_time + timedelta(seconds=time_diff)
    return NetLogRaw(
        user.subcriber_id,
        user.src_ip,
        user.src_port,
        dest.dest_ip,
        dest.dest_port,
        normalized_bytes(lag_time=time_diff),
        normalized_bytes(lag_time=time_diff),
        random.choice(TCP_FLAGS),
        start_time.strftime(TIME_FORMAT),
        end_time.strftime(TIME_FORMAT),
        dest.protocol_name,
        PROTOCOL_MAPPING.get(dest.protocol_name)
    )
    
def publish_events(publisher, topic_path, events):
    for event in events:
        json_str = json.dumps(event)
        data = json_str.encode('utf-8')
        publisher.publish(topic_path, data=data)

def create_event_stream(user, dest):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    num_events = random.randint(min_session_events, max_session_events)
    events = []
    while len(events) < num_events:
        start_time = datetime.now() if len(events) == 0 else datetime.strptime(events[-1]["end_time"],TIME_FORMAT)
        event = generate_event(start_time, user, dest)
        events.append(event._asdict())
    publish_events(publisher, topic_path, events)

if __name__ == "__main__":
    faker = Faker()
    dest_ips =  generate_destination_ips(num_dest_ip)
    # while True:
    for i in range(2):
        users = generate_users(nusers)
        user_dest_pair = get_user_destination_pair(users,dest_ips)
        with multiprocessing.Pool() as pool:
            pool.starmap(func=create_event_stream,iterable=user_dest_pair)
            pool.close()
            pool.join()
        time.sleep(5)