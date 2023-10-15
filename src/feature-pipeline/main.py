import argparse
from pipeline.netlog_feature import run


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build features from netlog data')
    # Google Cloud options
    parser.add_argument('--bucket',default="gs://electric-armor-395015-netlog-bucket")
    parser.add_argument('--file_name_suffix', default="json")
    parser.add_argument('--files', default="gs://electric-armor-395015-netlog-bucket/*/*.json")
    opts, pipeline_opts = parser.parse_known_args()
    run(opts, pipeline_opts)