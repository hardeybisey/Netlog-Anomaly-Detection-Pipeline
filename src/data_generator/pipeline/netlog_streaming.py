
import time
import json
import logging
import random
import apache_beam as beam
from utils.custom import NetworkPool, UserObject, JsonEvent
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions
        

class EventGenerator(beam.DoFn):
    def __init__(self, anomaly, max_events_per_session=20, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_events_per_session = max_events_per_session
        self.anomaly = anomaly
        self.network_pool = NetworkPool()
    
    def setup(self):
        self.user_obj = UserObject()    

    def process(self, element):
        network = self.network_pool.get_network()
        user = self.user_obj.get_user()
        num_events = random.randint(5, self.max_events_per_session)
        for _ in range(num_events):
            event = JsonEvent.generate(user, network, self.anomaly)
            yield event

def to_json(event):
    return json.dumps(event).encode('utf-8')

def run(args, beam_args):
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "Trigger" >> PeriodicImpulse(start_timestamp=time.time(), fire_interval=(60/args.qps))
        | "Generate Events" >> beam.ParDo(EventGenerator(args.anomaly))
        | "JSONIFY" >> beam.Map(to_json)
        | "Write to PubSub" >> beam.io.WriteToPubSub(args.topic)
    )
    return pipeline.run()