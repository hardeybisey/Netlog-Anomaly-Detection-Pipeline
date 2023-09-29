import json
import time
import typing
import apache_beam as beam
from datetime import datetime

class NetLogRaw(typing.NamedTuple):
    subscriberId : str
    srcIP : str
    dstIP : str
    srcPort : int
    dstPort : int
    txBytes : int
    rxBytes : int
    startTime : datetime
    endTime : datetime
    protocolName : str
    protocolNumber : int

class UniqueCombine(beam.CombineFn):
    def create_accumulator(self):
        return set()
    
    def add_input(self, accumulator, element):
        accumulator.add(element)
        return accumulator
    
    def merge_accumulators(self, accumulators):
        return set.union(*accumulators)
    
    def extract_output(self, accumulator):
        return list(accumulator)
    
class EventParser(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        try:
            event = json.loads(element.decode('utf-8'))
            event['timeStamp'] = timestamp.to_utc_datetime().isoformat()
            yield beam.pvalue.TaggedOutput('valid', event)
        except:
            yield beam.pvalue.TaggedOutput('invalid', element)

class JsonTOBeamRow(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S.%f"
        durationfmt = datetime.strptime(element['endTime'],timefmt) - datetime.strptime(element['startTime'],timefmt)
        yield beam.Row(
            duration=element['duration'],
            subscriberId=element['subscriberId'],
            srcIP=element['srcIP'],
            srcPort=element['srcPort'],
            dstIP=element['dstIP'],
            dstPort=element['dstPort'],
            tx_bytes=element['txBytes'],
            txBytes=element['rxBytes'],
            startTime=element['startTime'],
            endTime=element['endTime'],
            timeStamp=element['timeStamp'],
            protocolName=element['protocolName'],
            protocolNumber=element['protocolNumber'],
            duration=durationfmt.total_seconds())

pipe = beam.Pipeline()
rows = (pipe 
        | beam.io.ReadFromPubSub(topic=topic)
        | beam.ParDo(EventParser()).with_outputs('valid', 'invalid').with_output_types(NetLogRaw))

features = (
    rows.valid 
    | beam.ParDo(JsonTOBeamRow()) 
    | beam.GroupBy("subscriberId","dstIP")
        .aggregate_field("srcIP", UniqueCombine(), "number_of_unique_ips")
        .aggregate_field("srcPort", UniqueCombine(), "number_of_unique_ports")
        .aggregate_field("subscriberId", beam.combiners.CountCombineFn(),"number_of_records")
        .aggregate_field("txBytes", min ,"min_tx_bytes")
        .aggregate_field("txBytes", max ,"max_tx_bytes")
        .aggregate_field("txBytes", beam.combiners.MeanCombineFn() ,"avg_tx_bytes")
        .aggregate_field("rxBytes", min,"min_tx_bytes")
        .aggregate_field("rxBytes", max,"max_tx_bytes")
        .aggregate_field("rxBytes", beam.combiners.MeanCombineFn() ,"avg_tx_bytes")
        .aggregate_field("duration", min, "min_duration")
        .aggregate_field("duration", max, "max_duration")
        .aggregate_field("duration", beam.combiners.MeanCombineFn(), "avg_duration")
        # transaction_time
        # subscriber_id
        # dst_subnet
        # number_of_records
    )

predict_anomaly = (
    features
    | 'predict_anomaly'
    )


deadletters = rows.invalid | beam.io.WriteToBigQuery(deadletter_topic)

write_to_feature_store = (
    features
    | beam.io.WriteToBigQuery(
        table=table_id,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )