import json
import argparse
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from utils.custom import NetLogRawSchema, NetLogRowSchema, GetFeaturesFromRow,NetLogAggSchema

beam.coders.registry.register_coder(NetLogAggSchema, beam.coders.RowCoder)
beam.coders.registry.register_coder(NetLogRawSchema, beam.coders.RowCoder)
beam.coders.registry.register_coder(NetLogRowSchema, beam.coders.RowCoder)
netlogbqschema = json.loads(open("/Users/hardey/Desktop/GITHUB/AnomalyDetectionPipeline/beam-schema/net_log-raw.json").read())
    
    
class AvroEventParser(beam.DoFn):
    def process(self, element):
        data = element['data']
        try:
            event = json.loads(data.decode('utf-8'))
            event['publishTime'] = element['publish_time']
            yield beam.pvalue.TaggedOutput('valid', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalid', data)

class AssignTimeStamp(beam.DoFn):
    """
    Make each element a timestamp value
    """
    def process(self, element):
        timestamp = element.publishTime.timestamp()
        yield beam.window.TimestampedValue(element, timestamp)
        
            
class AddProcessingTime(beam.DoFn):
    """
    Add a processing time field to each element
    """
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
        element.ProcessingTime = datetime.now().strftime(timefmt)
        yield element
            
class JsonToBeamRow(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
        publish_time = element.publishTime.strftime(timefmt)
        duration = datetime.strptime(element.endTime, timefmt) - datetime.strptime(element.startTime, timefmt)
        yield (
            beam.Row(subscriberId=element.subscriberId,
                        srcIP=element.srcIP,
                        srcPort=element.srcPort,
                        dstIP=element.dstIP,
                        dstPort=element.dstPort,
                        txBytes=element.txBytes,
                        rxBytes=element.rxBytes,
                        startTime=element.startTime,
                        endTime=element.endTime,
                        publishTime=publish_time,
                        tcpFlag=element.tcpFlag,
                        protocolName=element.protocolName,
                        protocolNumber=element.protocolNumber,
                        duration=duration.total_seconds())
            )    

def run(args, beam_args):
    input_file_pattern = args.bucket + args.input_file_pattern
    valid_out_path = args.bucket + "/aggregate/anomaly/"
    invalid_out_path = args.bucket + "/badletters/"
    table_name = args.netlog_bq_table
    options = PipelineOptions(beam_args)
    pipeline =  beam.Pipeline(options=options)
    json_row = (
        pipeline
        |"Read From Text" >> beam.io.ReadFromAvro(file_pattern=input_file_pattern, min_bundle_size=0, validate=True)
        | "Parse Event" >> beam.ParDo(AvroEventParser()).with_outputs('valid', 'invalid').with_output_types(NetLogRawSchema)
    )

    row = (
        json_row.valid
        | "AsssignTimeStamp" >> beam.ParDo(AssignTimeStamp())
        | "ConvertToRow" >> beam.ParDo(JsonToBeamRow()).with_output_types(NetLogRowSchema)
        | "Add Processing Timestamp" >> beam.ParDo(AddProcessingTime())
    )

    features = (
        row
        | "FeaturesFixed1MinWindow" >> beam.WindowInto(beam.window.FixedWindows(60),
                                            allowed_lateness=beam.window.Duration(seconds=0),
                                            trigger = beam.trigger.AfterWatermark(),
                                            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
        | "GetFeaturesFromRow" >> GetFeaturesFromRow().with_output_types(NetLogAggSchema)
        | "WriteFeaturesToCloudStorage" >> beam.io.WriteToJson(path=valid_out_path)
    )

    # writerow = (
    #     row
    #     | "RawFixed2MinWindow" >> beam.WindowInto(beam.window.FixedWindows(120),
    #                                         allowed_lateness=beam.window.Duration(seconds=0),
    #                                         trigger = beam.trigger.AfterWatermark(),
    #                                         accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
    #     | "RowToDict" >> beam.Map(lambda x : x.as_dict())
    #     | "WriteRawLogsToBigquery" >> beam.io.WriteToBigQuery(table=table_name,
    #                                                 schema=netlogbqschema,
    #                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    #                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)   
    #     )

    # badletters =  (
    #     json_row.invalid
    #     | "WriteUnparsedToGCS" >>beam.io.fileio.WriteToFiles(path=invalid_out_path,shards=1,max_writers_per_bundle=0)
    # )
    return pipeline.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
    '--bucket', type = str,
    help='cloud storage bucket to write pipeline output',required=True)
        

    parser.add_argument(
    '--input_file_pattern',type=str,
    help='files to read from',required=True)

    parser.add_argument(
    '--netlog_bq_table',type=str,
    help='pub sub topic to read from',required=True)
    
    args, beam_args = parser.parse_known_args()
    run(args, beam_args)