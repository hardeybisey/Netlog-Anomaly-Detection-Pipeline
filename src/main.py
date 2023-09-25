import logging
from pipeline import netlog_streaming

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    options = netlog_streaming.StreamingPipelinOptions()
    netlog_streaming.run(options)