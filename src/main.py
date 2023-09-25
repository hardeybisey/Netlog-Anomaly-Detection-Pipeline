import logging
from pipeline import netlog_streaming , StreamingPipelinOptions

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    options = StreamingPipelinOptions()
    netlog_streaming.run(options)