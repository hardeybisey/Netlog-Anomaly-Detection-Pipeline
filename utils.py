import random
from faker import Faker
from typing import NamedTuple
from datetime import datetime, timedelta

class Network(NamedTuple):
    ipv4: str
    port: int
    protocol_name: str
    protocol_num: int
    
class User(NamedTuple):
    subscriber_id: str
    ipv4: str
    port: int


class NetworkPool:
    def __init__(self, num_dest_ip=10000):
        self.client = Faker()
        self.num_dest_ip = num_dest_ip
        self._protocols = [("TCP", 6), ("UDP", 17), ("HTTP", 80), ("HTTPS", 443)]
        self._ipv4 = self.generate_ipv4()
        
    def ipv4(self):
        return random.choice(self._ipv4)
     
    def port(self):
        return self.client.port_number("system")
    
    def protocol_info(self):
        return random.choice(self._protocols)
    
    def generate_ipv4(self):
        return [self.client.ipv4_private() for _ in range(self.num_dest_ip)]
    
    def get_network(self):
        protocol = self.protocol_info()
        return Network(self.ipv4(), self.port(), protocol[0], protocol[1])

class UserObject:
    def __init__(self):
        self.client = Faker()
        
    def subscriber_id(self):
        return self.client.uuid4()
    
    def ipv4(self):
        return self.client.ipv4()
    
    def port(self):
        return self.client.port_number("user")
    
    def get_user(self):
        return User(self.subscriber_id(), self.ipv4(), self.port())

class JsonEvent:
    max_request_bytes = 5000
    
    @classmethod
    def generate(cls, user, network):
        start_time = datetime.now()
        time_diff = random.uniform(0, 5 * 2)
        end_time = start_time + timedelta(seconds=time_diff)
        return {"subscriber_id": user.subscriber_id,
                "src_ip": user.ipv4,
                "src_port": user.port,
                "dest_ip": network.ipv4,
                "dst_port": network.port,
                "tx_bytes": cls.normalized_bytes(time_diff),
                "rx_bytes": cls.normalized_bytes(time_diff),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "protocol_name": network.protocol_name,
                "protocol_num": network.protocol_num}
        
    @staticmethod
    def normalized_bytes(lag_time):
        random_byte = random.uniform(10, 100)
        normalized_byte = random_byte * lag_time
        return int(min(normalized_byte, JsonEvent.max_request_bytes))