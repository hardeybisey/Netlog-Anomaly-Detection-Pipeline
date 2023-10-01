import json
import time
import logging
import apache_beam as beam
from datetime import datetime
from utils import NetLogRawSchema, NetLogAggSchema, UniqueCombine, JsonTOBeamRow

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

def ip_to_subnet(ip):
    return '.'.join(ip.split('.')[:3]) + '.0/24'


pipe = beam.Pipeline()
row = (pipe 
        |"Read From Pub/Sub" >> beam.io.ReadFromPubSub(topic="projects/electric-armor-395015/topics/netlog-stream", subscription="projects/electric-armor-395015/subscriptions/netlog-stream-sub", with_attributes=True)
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

predict_anomaly = (
    features
    | 'predict_anomaly' >> beam.ParDo(PredictAnomaly()).with_output_types(NetLogAggSchema)
    |  "write Anomaly to BQ" >> beam.io.WriteToBigQuery(
        table="table_id",
        schema="table_schema",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )


deadletters = (
    row.invalid | beam.io.WriteToBigQuery(
        table="table_id",
        schema="table_schema",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
)
    
write_to_feature_store = (
    features |"Write Features to BQ" >>beam.io.WriteToBigQuery(
        table="table_id",
        schema="table_schema",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )
)