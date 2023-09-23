#!/usr/bin/env python3

import uuid
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
parser.add_argument("--ndest_ips", type=int, help="The number of unique destination IPs", default=1000)
parser.add_argument("--nusers", type=int, help="The number of users", default=15000)
parser.add_argument("--avg_requests_secs", type=int, help="The avg time between requests", default=10)
parser.add_argument("--max_request_bytes", type=int, help="The max bytes for each user request", default=5000)
parser.add_argument("--min_session_events",type=int, help="Minumim number of events per session", default=5)
parser.add_argument("--max_session_events", type=int, help="Maximim number of events per session", default=20)
parser.add_argument("--ncycles", type=int, help="Number of cycles", default=1)


args = parser.parse_args()
nusers = args.nusers
ncycles = args.ncycles
num_dest_ip = args.ndest_ips
max_request_bytes = args.max_request_bytes
avg_requests_secs = args.avg_requests_secs
min_session_events = args.min_session_events
max_session_events = args.max_session_events
    
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
    return [Destination(faker.ipv4_private(), faker.port_number(), random.choice(PROTOCOL_NAMES)) for _ in range(num_dest_ip)]

def get_user_destination_pair(users, dest):
    return list((user,random.choice(dest)) for user in users)

def normalized_bytes(lag_time):
    random_byte = random.uniform(50, 300)
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
    
def publish_events(events):
    file_name = "data" + "/" +str(uuid.uuid4()) + ".json"
    with open(file_name, "w") as file:
        json_data = json.dumps(events)
        file.write(json_data)

def create_event_stream(user, dest):
    num_events = random.randint(min_session_events, max_session_events)
    events = []
    while len(events) < num_events:
        start_time = datetime.now() if len(events) == 0 else datetime.strptime(events[-1]["end_time"],TIME_FORMAT)
        event = generate_event(start_time, user, dest)
        events.append(event._asdict())
    publish_events(events)

if __name__ == "__main__":
    faker = Faker()
    dest_ips =  generate_destination_ips(num_dest_ip)
    for _ in range(ncycles):
        users = generate_users(nusers)
        user_dest_pair = get_user_destination_pair(users,dest_ips)
        with multiprocessing.Pool() as pool:
            pool.starmap(func=create_event_stream,iterable=user_dest_pair)
            pool.close()
            pool.join()