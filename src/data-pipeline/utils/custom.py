import random
from faker import Faker
from typing import NamedTuple
from datetime import datetime, timedelta

client = Faker()

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
        self._client = client
        self._num_dest_ip = num_dest_ip
        self._protocols = [("TCP", 6), ("UDP", 17), ("HTTP", 80), ("HTTPS", 443)]
        self._ip = self._generate_ipv4()
        
    def _ipv4(self):
        return random.choice(self._ip)

    def _port(self):
        return random.randint(1000, 5000)
    
    def _protocol_info(self):
        return random.choice(self._protocols)
    
    def _generate_ipv4(self):
        return [self._client.ipv4_private() for _ in range(self._num_dest_ip)]
    
    def get_network(self):
        protocol = self._protocol_info()
        return Network(self._ipv4(), self._port(), protocol[0], protocol[1])

class UserObject:
    def __init__(self):
        self.client = client
        
    def _subscriber_id(self):
        return self.client.uuid4()
    
    def _ipv4(self):
        return self.client.ipv4()
    
    def _port(self):
        return random.randint(1000, 5000)
    
    def get_user(self):
        return User(self._subscriber_id(), self._ipv4(), self._port())
    
class JsonEvent:
    max_request_bytes = 5000
    allowed_lag_sec = 10
    tcp_flag = ["SYN", "ACK", "FIN", "RST", "PSH", "URG"]
    
    @classmethod
    def generate(cls, user, network, anomaly):
        start_time = datetime.now()
        time_diff = random.uniform(0, cls.allowed_lag_sec)
        end_time = start_time + timedelta(seconds=time_diff)
        return {"subscriberId": user.subscriber_id,
                "srcIP": user.ipv4,
                "srcPort": user.port,
                "dstIP": network.ipv4,
                "dstPort": network.port,
                "txBytes": cls._normalized_bytes(time_diff, anomaly),
                "rxBytes": cls._normalized_bytes(time_diff, anomaly),
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "tcpFlag": random.choice(cls.tcp_flag),
                "protocolName": network.protocol_name,
                "protocolNumber": network.protocol_num}
        
    @classmethod
    def _normalized_bytes(cls, lag_time, anomaly):
        random_byte = random.uniform(10, 100)
        normalized_byte = int(min((random_byte * lag_time), cls.max_request_bytes))
        return normalized_byte if not anomaly else normalized_byte * 10