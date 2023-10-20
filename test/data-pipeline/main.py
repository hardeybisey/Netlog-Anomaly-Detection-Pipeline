import argparse
from pipeline.netlog_streaming import run
from apache_beam.internal import pickler
pickler.set_library(pickler.USE_CLOUDPICKLE)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--topic', type = str,
        help='pubsub topic for the pipeline to publish to',required=True)
            
    parser.add_argument(
        '--qps',type=int,
        help='the qps to generate events at',required=True)
    
    parser.add_argument(
        '--event_type',type=str,
        help='type of event to generate, possible options are normal and anomaly',required=True)

    args, beam_args = parser.parse_known_args()
    run(args, beam_args)