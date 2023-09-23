
import time
import json
import random
import apache_beam as beam
from utils import JsonEvent, UserObject, NetworkPool
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.transforms.trigger import Repeatedly, AfterCount
# from apache_beam.transforms.window import GlobalWindows
# from apache_beam.options.pipeline_options import PipelineOptions

class StreamingPipelinOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--topic', type = str,
            help='pubsub topic for the pipeline to publish to',required=True)
        
        parser.add_argument(
            '--qps',type=int,
            help='the qps to generate events at',required=True)
        
# python3 pipeline.py --streaming --autoscaling_algorithm NONE --num_workers 5 --max_num_workers 5 --enable_streaming_engine True --project "${PROJECT_ID}" --topic "projects/${PROJECT_ID}/topics/test" --qps 10 --runner "DataflowRunner" --region europe-west2 temp_location gs://${BUCKET}/temp staging_location gs://${BUCKET}/staging
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
    
if __name__ == '__main__':
    options = StreamingPipelinOptions()
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    run(options)