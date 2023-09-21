#!/Users/hardey/Desktop/GITHUB/AnomalyDetectionPipeline/apache-beam/bin/python3

import time
import json
import random
import argparse
from faker import Faker
from dataclasses import dataclass
from datetime import datetime, timedelta
from google.cloud import pubsub_v1


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TCP_FLAGS = ["URG", "ACK", "PSH", "RST", "SYN", "FIN"]
PROTOCOL_NAMES = ["TCP", "UDP", "HTTP", "HTTPS"]
protocol_mapping = {"TCP": 6, "UDP": 17, "HTTP": 80, "HTTPS": 443}
project_id=None 
topic_name= None

@dataclass
class User:
    subcriber_id : str
    src_ip : str
    src_port: int
    
@dataclass
class Destination:
    dest_ip : str
    dest_port : int
    protocol_name : str
    protocol_num : int
            
@dataclass
class NetLogRaw:
    subscriberId : str
    srcIP : str
    dstIP : str
    srcPort : int
    dstPort : int
    txBytes : int
    rxBytes : int
    tcpFlag : int
    startTime : datetime
    endTime : datetime
    protocolName : str
    protocolNumber : int


def generate_users(num_users):
    return [User(faker.uuid4(), faker.ipv4(),faker.port_number()) for _ in range(num_users)]
    
def generate_destination_ips(num_dest_ip):
    proto_col_name = random.choice(PROTOCOL_NAMES)
    protocol_num = protocol_mapping[proto_col_name]
    return [Destination(faker.ipv4_private(), faker.port_number(), proto_col_name, protocol_num) for _ in range(num_dest_ip)]

def normalized_bytes(lag_time, max_bytes):
    random_byte = random.randint(10, 100)
    normalized_byte = random_byte * lag_time
    return min(normalized_byte, max_bytes)

def publish_burst(publisher, topic_path, burst):
    for event_dict in burst:
        json_str = json.dumps(event_dict)
        data = json_str.encode('utf-8')
        publisher.publish(topic_path, data=data)
        
def generate_event(start_time, user, dest, avg_time_between_requests, max_bytes):
        time_diff = random.uniform(0, avg_time_between_requests * 2)
        end_time = start_time + timedelta(seconds=time_diff)
        return NetLogRaw(
            user.subcriber_id,
            user.src_ip,
            user.src_port,
            dest.dest_ip,
            dest.dest_port, 
            normalized_bytes(time_diff, max_bytes), 
            normalized_bytes(time_diff, max_bytes), 
            random.choice(TCP_FLAGS), 
            start_time.strftime(TIME_FORMAT), 
            end_time.strftime(TIME_FORMAT), 
            dest.protocol_name, 
            dest.protocol_num            
        )
        
def create_process(user, dest, avg_time_between_requests, max_bytes, max_events_per_burst):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    num_events = random.randint(0, max_events_per_burst)
    burst = []
    while len(burst) < num_events:
        start_time = datetime.now() if len(burst) == 0 else burst[-1]['endTime'] 
        burst.append(generate_event(start_time, user, dest, avg_time_between_requests, max_bytes))
    publish_burst(publisher, topic_path, burst)
     
if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
    parser.add_argument("--num_events", "-e", type=int, dest="num_events", help="The number of events", default=1000000)
    parser.add_argument("--unique_dest_ips", type=int, dest="ips", help="The number of unique destination IPs", default=1000)
    parser.add_argument("--avg_time_between_requests", "-time", type=int, dest="avg_time_between_requests", help="The avg time between requests", default=10)
    parser.add_argument("--num_users", "-u", type=int, dest="num_users", help="The number of users", default=10000)
    parser.add_argument("--max_bytes", "-b", type=str, dest="max_bytes", help="The output file", default=5000)

    args = parser.parse_args()
    
    max_bytes = args.max_bytes
    num_users = args.num_users   
    num_events = args.num_events
    num_unique_dest_ip = args.unique_dest_ips   
    avg_time_between_requests = args.avg_time_between_requests
    
    

    users = [generate_user() for _ in range(num_users)]
    dest_ips = generate_destination_ips(num_unique_dest_ip)
    


    users = json.loads(open("users.json").read())
    dest_ips = json.loads(open("dest_ips.json").read())
    
if __name__ == "__main__":
    faker = Faker()
    parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
    parser.add_argument("--num_users", "-e", type=int, dest="num_users", help="The number of users to create", default=10000)
    parser.add_argument("--output", "-o", type=str, dest="output", help="The output file", default="users.json")
    
    args = parser.parse_args()
    
    users = [generate_user() for _ in range(args.num_users)]
    users_to_json(users, args.output)




# faker = Faker()

# parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
# parser.add_argument("--num_events", "-e", type=int, dest="num_events", help="The number of events to create", default=100000)
# parser.add_argument("--num_unique_dest_ip", "-ip", type=int, dest="num_unique_dest_ip", help="The number of unique destination IPs", default=1000)
# parser.add_argument("--min_time_between_requests", "-l", type=int, dest="min_time_between_requests", help="The minimum time between requests", default=10)

