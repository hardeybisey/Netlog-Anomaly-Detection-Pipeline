from dataclasses import dataclass
from faker import Faker
import random
import argparse
from datetime import datetime, timedelta

faker = Faker()

parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
parser.add_argument("--num_events", "-e", type=int, dest="num_events", help="The number of events to create", default=100000)
parser.add_argument("--num_unique_dest_ip", "-ip", type=int, dest="num_unique_dest_ip", help="The number of unique destination IPs", default=1000)
parser.add_argument("--min_time_between_requests", "-l", type=int, dest="min_time_between_requests", help="The minimum time between requests", default=10)


min_time_between_requests = 10
num_unique_dest_ip= 1000


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



class NetLogGenerator:
    def __init__(self, min_time_between_requests,num_events=1000, num_dest_ip=1000, max_bytes=3000):
        self.min_lag_time = min_time_between_requests
        self.num_events = num_events
        self.max_bytes = max_bytes
        self.max_bytes = 3000
        self.date_fmt = "%Y-%m-%d %H:%M:%S"
        self.dest_ips = self.generate_destination_ip(num_dest_ip)

    def generate_netlog(self):
        fmt = self.date_fmt
        subcriber_id = faker.uuid4()
        src_ip = faker.ipv4()
        dest_ip = random.choice(self.dest_ips)
        src_port = faker.port_number()
        dest_port = faker.port_number()
        time_diff = random.uniform(0, self.min_lag_time * 2)
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=time_diff)
        tx_bytes = self.normalized_bytes(time_diff, self.max_bytes)
        rx_bytes = self.normalized_bytes(time_diff, self.max_bytes)
        tcp_flag = random.choice(["URG", "ACK", "PSH", "RST", "SYN", "FIN"])
        protocol_name = random.choice(["TCP", "UDP", "HTTP", "HTTPS"])
        protocol_number = self.protocol_dict()[protocol_name]
        return NetLogRaw(subcriber_id, src_ip, dest_ip, src_port, dest_port, tx_bytes, rx_bytes, tcp_flag, start_time.strftime(fmt), end_time.strftime(fmt), protocol_name, protocol_number)
        
    def normalized_bytes(self, lag_time, max_bytes):
        random_byte = random.randint(10, 100)
        # Normalize the random byte value by a factor of the lag time (larger lag time makes the byte larger)
        normalized_byte = random_byte * lag_time
        # Cap the normalized byte value to the specified maximum value
        return min(normalized_byte, max_bytes)

    def generate_destination_ip(self, num_dest_ip):
        return list(faker.ipv4_private() for _ in range(num_dest_ip))
    
    def protocol_dict(self):
        return {"TCP": 6, "UDP": 17, "HTTP": 80, "HTTPS": 443}
    
    def run(self):
        for _ in range(self.num_events):
            yield self.generate_netlog()
            
if __name__ == "__main__":
    pass