import argparse
from pipeline.netlog_feature import run
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.custom import NetLogRawSchema, NetLogRowSchema
from utils.custom import UniqueCombine, EventParser, JsonToBeamRow, AddTimeStamp, AddProcessingTime


import typing
import json
import apache_beam as beam
from datetime import datetime

class NetLogRawSchema(typing.NamedTuple):
    subscriberId : str
    srcIP : str
    dstIP : str
    srcPort : int
    dstPort : int
    txBytes : int
    rxBytes : int
    startTime : datetime
    endTime : datetime
    tcpFlag : str
    protocolName : str
    protocolNumber : int


class NetLogRowSchema(typing.NamedTuple):
    subscriberId: str
    srcIP: str
    srcPort: int
    dstIP: str
    dstPort: int
    txBytes: int
    rxBytes: int
    startTime: datetime
    endTime: datetime
    duration: float
    
class UniqueCombine(beam.CombineFn):
    def create_accumulator(self):
        return set()
    
    def add_input(self, accumulator, element):
        accumulator.add(element)
        return accumulator
    
    def merge_accumulators(self, accumulators):
        return set.union(*accumulators)
    
    def extract_output(self, accumulator):
        return len(accumulator)

class EventParser(beam.DoFn):
    def process(self, element):
        try:
            event = json.loads(element)
            yield beam.pvalue.TaggedOutput('validjson', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalidjson', element)

class AddTimeStamp(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S.%f"
        timestamp = datetime.strptime(element.endTime, timefmt).timestamp()
        yield beam.window.TimestampedValue(element, timestamp)
        
class AddProcessingTime(beam.DoFn):
    def process(self, element):
        element = element._asdict()
        element['ProcessingTime'] = datetime.now().isoformat()
        yield element
            
class JsonToBeamRow(beam.DoFn):
    def process(self, element):
        try:
            timefmt = "%Y-%m-%dT%H:%M:%S.%f"
            duration = datetime.strptime(element.endTime, timefmt) - datetime.strptime(element.startTime, timefmt)
            row = beam.Row(subscriberId=element.subscriberId,
                            srcIP=element.srcIP,
                            srcPort=element.srcPort,
                            dstIP=element.dstIP,
                            dstPort=element.dstPort,
                            txBytes=element.txBytes,
                            rxBytes=element.rxBytes,
                            startTime=element.startTime,
                            endTime=element.endTime,
                            duration=duration.total_seconds())
            yield beam.pvalue.TaggedOutput('validrow', row)
        except:
            yield beam.pvalue.TaggedOutput('invalidrow', element)
            
            
beam.coders.registry.register_coder(NetLogRawSchema, beam.coders.RowCoder)
beam.coders.registry.register_coder(NetLogRowSchema, beam.coders.RowCoder)
    
class GetFeaturesFromRow(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "With timestamps" >> beam.ParDo(AddTimeStamp())
            | "Fixed Window 1 Min" >> beam.WindowInto(beam.window.FixedWindows(120),
                              allowed_lateness=beam.window.Duration(seconds=0),
                              trigger = beam.trigger.AfterWatermark(),
                              accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
            | "Aggregate Row" >> beam.GroupBy("subscriberId","dstIP")
                        .aggregate_field("srcIP", UniqueCombine(), "UniqueIPs")
                        .aggregate_field("srcPort", UniqueCombine(), "UniquePorts")
                        .aggregate_field("subscriberId", beam.combiners.CountCombineFn(),"NumRecords")
                        .aggregate_field("txBytes", min ,"MinTxBytes")
                        .aggregate_field("txBytes", max ,"MaxTxBytes")
                        .aggregate_field("txBytes", beam.combiners.MeanCombineFn() ,"AvgTxBytes")
                        .aggregate_field("rxBytes", min,"MinRxBytes")
                        .aggregate_field("rxBytes", max,"MaxRxBytes")
                        .aggregate_field("rxBytes", beam.combiners.MeanCombineFn() ,"AvgRxBytes")
                        .aggregate_field("duration", min, "MinDuration")
                        .aggregate_field("duration", max, "MaxDuration")
                        .aggregate_field("duration", beam.combiners.MeanCombineFn(), "AvgDuration")
            | "Add Processing Timestamp" >> beam.ParDo(AddProcessingTime())
               )

def run(opts, beam_args):   
    valid_out_path = opts.bucket + "/aggregate/"
    invalid_out_path_row = opts.bucket + "/badletters/row/"
    invalid_out_path_json = opts.bucket + "/badletters/json/"
    options = PipelineOptions(beam_args, save_main_session=True)
    pipeline =  beam.Pipeline(options=options)
    json_rows = (
    pipeline
    |"Read From Text" >> beam.io.ReadFromText(opts.files)
    | "Parse Event" >> beam.ParDo(EventParser()).with_outputs('validjson', 'invalidjson').with_output_types(NetLogRawSchema)
    )

    beam_rows = (
        json_rows.validjson
        | "Convert To Row" >> beam.ParDo(JsonToBeamRow()).with_outputs('validrow', 'invalidrow').with_output_types(NetLogRowSchema)
    )

    features = (
        beam_rows.validrow
        | "Get Features" >> GetFeaturesFromRow()
        | "Write Features To Cloud Storage" >> beam.io.WriteToText(file_path_prefix=valid_out_path,file_name_suffix=opts.file_name_suffix)
    )

    badjson =  (
        json_rows.invalidjson
        | "Write Bad Json To Cloud Storage" >>beam.io.WriteToText(file_path_prefix=invalid_out_path_json,file_name_suffix=opts.file_name_suffix)
    )
    
    badrows = (
        beam_rows.invalidrow
        | "Write Bad Row To Cloud Storage" >>beam.io.WriteToText(file_path_prefix=invalid_out_path_row,file_name_suffix=opts.file_name_suffix)
        
    )
    

    return pipeline.run()




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build features from netlog data')
    parser.add_argument('--bucket',default="gs://electric-armor-395015-netlog-bucket")
    parser.add_argument('--file_name_suffix', default=".json")
    parser.add_argument('--files', default="gs://electric-armor-395015-netlog-bucket/*/*.json")
    opts, pipeline_opts = parser.parse_known_args()
    run(opts, pipeline_opts)