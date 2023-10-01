import json
import time
import logging
import typing
import datetime
import apache_beam as beam
import argparse
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

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
    timeStamp : datetime
    protocolName : str
    protocolNumber : int

class NetLogAggSchema(typing.NamedTuple):
    ProcessingTime : datetime
    subscriberId : str
    dstIP : str
    UniqueIPs : int
    UniquePorts : int
    Records : int
    MinTxBytes: int
    MaxTxBytes: int
    AvgTxBytes: float
    MinRxBytes: int
    MaxRxBytes: int
    AvgRxBytes: float
    MinDuration: float
    MaxDuration: float
    AvgDuration: float
    
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
    
class JsonTOBeamRow(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S.%f"
        duration = datetime.strptime(element['endTime'],timefmt) - datetime.strptime(element['startTime'],timefmt)
        yield beam.Row(
            subscriberId=element['subscriberId'],
            srcIP=element['srcIP'],
            srcPort=element['srcPort'],
            dstIP=element['dstIP'],
            dstPort=element['dstPort'],
            tx_bytes=element['txBytes'],
            txBytes=element['rxBytes'],
            startTime=element['startTime'],
            endTime=element['endTime'],
            duration=duration.total_seconds())

class EventParser(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        try:
            event = json.loads(element.decode('utf-8'))
            element['timeStamp'] = timestamp.to_utc_datetime().isoformat()
            yield beam.pvalue.TaggedOutput('valid', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalid', element.decode('utf-8'))

class AddProcessingTime(beam.DoFn):
    def process(self, element):
        element = element._asdict()
        element['ProcessingTime'] = datetime.now().isoformat()
        yield element

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
pipe = beam.Pipeline(options=options)
row = (pipe 
        |"Read From Pub/Sub" >> beam.io.ReadFromPubSub(topic="projects/electric-armor-395015/topics/netlog-stream",with_attributes=True)
        |"Parse Event" >> beam.ParDo(EventParser()).with_outputs('valid', 'invalid').with_output_types(NetLogRawSchema))


features = (row.valid
            |"Convert To Row" >> beam.ParDo(JsonTOBeamRow()) 
            |"Aggregate Row" >> beam.GroupBy("subscriberId","dstIP")
                                    .aggregate_field("srcIP", UniqueCombine(), "UniqueIPs")
                                    .aggregate_field("srcPort", UniqueCombine(), "UniquePorts")
                                    .aggregate_field("subscriberId", beam.combiners.CountCombineFn(),"Records")
                                    .aggregate_field("txBytes", min ,"MinTxBytes")
                                    .aggregate_field("txBytes", max ,"MaxTxBytes")
                                    .aggregate_field("txBytes", beam.combiners.MeanCombineFn() ,"AvgTxBytes")
                                    .aggregate_field("rxBytes", min,"MinRxBytes")
                                    .aggregate_field("rxBytes", max,"MaxRxBytes")
                                    .aggregate_field("rxBytes", beam.combiners.MeanCombineFn() ,"AvgRxBytes")
                                    .aggregate_field("duration", min, "MinDuration")
                                    .aggregate_field("duration", max, "MaxDuration")
                                    .aggregate_field("duration", beam.combiners.MeanCombineFn(), "AvgDuration")
            |"Add Timestamp Value" >> beam.ParDo(AddProcessingTime()).with_output_types(NetLogAggSchema)
            )

write_to_feature_store = (
    features |"Write Features to BQ" >>beam.io.WriteToBigQuery(
        table="electric-armor-395015.netlog_dataset.features",
        schema="ProcessingTime:TIMESTAMP, subscriberId:STRING, dstIP:STRING, UniqueIPs:INTEGER, UniquePorts:INTEGER, Records:INTEGER, MinTxBytes:INTEGER, MaxTxBytes:INTEGER, AvgTxBytes:FLOAT, MinRxBytes:INTEGER, MaxRxBytes:INTEGER, AvgRxBytes:FLOAT, MinDuration:FLOAT, MaxDuration:FLOAT, AvgDuration:FLOAT",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )
)

write_to_cloud_storage = (
    row.valid 
    | " Windowing" >> beam.WindowInto(beam.window.FixedWindows(600),
                                      trigger=beam.trigger.AfterWatermark(late=beam.trigger.AfterCount(1)),
                                      accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
    |"Write to Cloud Storage" >> beam.io.WriteToText("gs://netlog-streaming-dataflow/bacth", file_name_suffix=".json")
    )
if __name__ == "__main__":
    pipe.run()
    
    
# python test.py  --project electric-armor-395015 --region europe-west2 --temp_location gs://${BUCKET}/tmp --staging_location gs://${BUCKET}/staging --job_name aggregate-data --max_num_workers 1 --worker_machine_type n1-standard-4 --runner DataFlowRunner