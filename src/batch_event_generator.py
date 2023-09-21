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

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TCP_FLAGS = ["URG", "ACK", "PSH", "RST", "SYN", "FIN"]
PROTOCOL_NAMES = ["TCP", "UDP", "HTTP", "HTTPS"]
PROTOCOL_MAPPING = {"TCP": 6, "UDP": 17, "HTTP": 80, "HTTPS": 443}


parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
parser.add_argument("--unique_dest_ips","-dest_ip_num", type=int, help="The number of unique destination IPs", default=200)
parser.add_argument("--num_users", "-u", type=int, dest="num_users", help="The number of users", default=15)
parser.add_argument("--avg_sec_between_requests","-req_avg_sec", type=int, help="The avg time between requests", default=10)
parser.add_argument("--max_bytes_per_request","-req_max_byte", type=int, help="The max bytes for each user request", default=5000)
parser.add_argument("--max_lag_millis", type=int, help="The max lag between each user session", default=500)
parser.add_argument("--min_events_per_session", "-min_event", type=int, help="Minumim number of events per session", default=5)
parser.add_argument("--max_events_per_session", "-max_event", type=int, help="Maximim number of events per session", default=20)
parser.add_argument("--file_path", "-fp", type=str, help="GCP project id", default="data")
parser.add_argument("--topic_name", "-t", type=str, help="pubsub topic", default="mytopic")


args = parser.parse_args()
file_path = args.file_path
topic_name = args.topic_name
num_users = args.num_users
num_dest_ip = args.unique_dest_ips
max_lag_millis = args.max_lag_millis
max_bytes_per_request = args.max_bytes_per_request
avg_sec_between_requests = args.avg_sec_between_requests
min_events_per_session = args.min_events_per_session
max_events_per_session = args.max_events_per_session
    
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

def generate_users(num_users):
    return [User(faker.uuid4(), faker.ipv4(), faker.port_number()) for _ in range(num_users)]

def generate_destination_ips(num_dest_ip):
    return [Destination(faker.ipv4_private(), faker.port_number(), random.choice(PROTOCOL_NAMES)) for _ in range(num_dest_ip)]

def get_user_destination_pair(users, dest):
    return list((user,random.choice(dest)) for user in users)

def normalized_bytes(lag_time):
    random_byte = random.uniform(50, 300)
    normalized_byte = random_byte * lag_time
    return int(min(normalized_byte, max_bytes_per_request))

def publish_events(events):
    pid = str(os.getpid())
    file_name = file_path + "/" + pid + ".csv"
    with open(file_name, "w") as file:
        for event in events:
            json_str = json.dumps(event._asdict())
            file.write(json_str + "\n")

def sleep_then_publish_events(events):
    sleep_secs = random.uniform(0, max_lag_millis/1000)
    time.sleep(sleep_secs)
    publish_events(events)

def generate_event(start_time, user, dest):
    time_diff = random.uniform(0, avg_sec_between_requests * 2)
    end_time = start_time + timedelta(seconds=time_diff)
    return NetLogRaw(user.subcriber_id,
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
                     PROTOCOL_MAPPING.get(dest.protocol_name),
                     )

def create_event_stream(user, dest):
    num_events = random.randint(min_events_per_session, max_events_per_session)
    events = []
    while len(events) < num_events:
        start_time = datetime.now() if len(events) == 0 else datetime.strptime(events[-1].end_time,TIME_FORMAT)
        events.append(generate_event(start_time, user, dest))
    sleep_then_publish_events(events)

if __name__ == "__main__":
    faker = Faker()
    dest_ips =  generate_destination_ips(num_dest_ip)
    users = generate_users(num_users)
    user_dest_pair = get_user_destination_pair(users,dest_ips)
    with multiprocessing.Pool() as pool:
        pool.starmap(func=create_event_stream,iterable=user_dest_pair)
        pool.close()
        pool.join()