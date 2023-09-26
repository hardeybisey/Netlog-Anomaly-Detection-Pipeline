import logging
import argparse
from pipeline.netlog_streaming import run

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
                '--topic', type = str,
                help='pubsub topic for the pipeline to publish to',required=True)
            
    parser.add_argument(
        '--qps',type=int,
        help='the qps to generate events at',required=True)

    args, beam_args = parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    run(args, beam_args)