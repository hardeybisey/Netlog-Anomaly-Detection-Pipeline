import argparse
from pipeline.netlog_feature import run

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket', type = str,
        help='cloud storage bucket to write pipeline output',required=True)
            
    parser.add_argument(
        '--file_name_suffix',type=str,
        help='suffix for to use for files written to cloud storage',required=True)
    
    parser.add_argument(
        '--topic',type=str,
        help='pub sub topic to read from',required=True)
    
    parser.add_argument(
        '--netlog_bq_table',type=str,
        help='pub sub topic to read from',required=True)

    args, beam_args = parser.parse_known_args()
    run(args, beam_args)