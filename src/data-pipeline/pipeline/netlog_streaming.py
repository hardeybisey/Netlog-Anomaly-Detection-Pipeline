import random
import apache_beam as beam
from utils.custom import NetworkPool, UserObject, JsonEvent, to_json 
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions

class EventGenerator(beam.DoFn):
    def __init__(self, event_type, max_events_per_session=20):
        self.max_events_per_session = max_events_per_session
        self.event_type = event_type
    
    def setup(self):
        self.user_obj = UserObject()    
        self.network_pool = NetworkPool()  

    def process(self, element):
        network = self.network_pool.get_network()
        user = self.user_obj.get_user()
        num_events = random.randint(5, self.max_events_per_session)
        for _ in range(num_events):
            event = JsonEvent.generate(user, network, self.event_type)
            yield event

def run(args, beam_args):
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "PeriodicImpulse" >> PeriodicImpulse(fire_interval=(60/int(args.qps)))
        | "Generate Events" >> beam.ParDo(EventGenerator(args.event_type))
        | "JSONIFY" >> beam.Map(to_json)
        | "Write to PubSub" >> beam.io.WriteToPubSub(args.topic)
    )
    return pipeline.run()