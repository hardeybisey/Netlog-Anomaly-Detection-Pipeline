
import time
import json
import random
import logging
from faker import Faker
import apache_beam as beam
from typing import NamedTuple
from datetime import datetime, timedelta
# from utils import JsonEvent, UserObject, NetworkPool
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions

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
        return self._client.port_number("system")
    
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
        return self.client.port_number("user")
    
    def get_user(self):
        return User(self._subscriber_id(), self._ipv4(), self._port())

class JsonEvent:
    max_request_bytes = 5000
    allowed_lag_sec = 10
    
    @classmethod
    def generate(cls, user, network):
        start_time = datetime.now()
        time_diff = random.uniform(0, cls.allowed_lag_sec)
        end_time = start_time + timedelta(seconds=time_diff)
        return {"subscriber_id": user.subscriber_id,
                "src_ip": user.ipv4,
                "src_port": user.port,
                "dest_ip": network.ipv4,
                "dst_port": network.port,
                "tx_bytes": cls._normalized_bytes(time_diff),
                "rx_bytes": cls._normalized_bytes(time_diff),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "protocol_name": network.protocol_name,
                "protocol_num": network.protocol_num}
        
    @classmethod
    def _normalized_bytes(cls, lag_time):
        random_byte = random.uniform(10, 100)
        normalized_byte = random_byte * lag_time
        return int(min(normalized_byte, cls.max_request_bytes))


class StreamingPipelinOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--topic', type = str,
            help='pubsub topic for the pipeline to publish to',required=True)
        
        parser.add_argument(
            '--qps',type=int,
            help='the qps to generate events at',required=True)
        
def to_json(event):
    return json.dumps(event).encode('utf-8')        

class EventGenerator(beam.DoFn):
    def __init__(self,max_events_per_session=20):
        self.max_events_per_session = max_events_per_session
        self.network_pool = NetworkPool()
    
    def setup(self):
        self.user_obj = UserObject()    

    def process(self, element, window=beam.DoFn.WindowParam):
        network = self.network_pool.get_network()
        user = self.user_obj.get_user()
        num_events = random.randint(5, self.max_events_per_session)
        for _ in range(num_events):
            event = JsonEvent.generate(user, network)
            yield beam.window.TimestampedValue(event, window.start)
                        
def run(options):
    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "Trigger" >> PeriodicImpulse(start_timestamp=time.time(), fire_interval=(60/options.qps))
        | "Generate Events" >> beam.ParDo(EventGenerator())
        | "JSONIFY" >> beam.Map(to_json)
        | "Write to PubSub" >> beam.io.WriteToPubSub(options.topic)
    )
    return pipeline.run()

# python3 pipeline.py --streaming --autoscaling_algorithm NONE --num_workers 1 --max_num_workers 1 --enable_streaming_engine True --project "${PROJECT_ID}" --topic "projects/${PROJECT_ID}/topics/test" --qps 10 --runner "DataflowRunner" --region europe-west2 temp_location gs://${BUCKET}/temp staging_location gs://${BUCKET}/staging    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    options = StreamingPipelinOptions()
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    run(options)