import json
import time
import logging
import apache_beam as beam
from datetime import datetime
from utils import NetLogRawSchema, NetLogAggSchema, NetLogFeaturesSchema, UniqueCombine, JsonToBeamRow, EventParser, AddProcessingTime


pipeline = beam.Pipeline()
row = (pipeline 
        |"Read From Pub/Sub" >> beam.io.ReadFromPubSub(topic="projects/electric-armor-395015/topics/netlog-stream")
        |"Parse Event" >> beam.ParDo(EventParser()).with_outputs('valid', 'invalid').with_output_types(NetLogRawSchema))


features = (row.valid
            |"Convert To Row" >> beam.ParDo(JsonToBeamRow()) 
            | "Fixed Window 1 Min" >> beam.WindowInto(beam.window.FixedWindows(60),
                                          allowed_lateness=beam.window.Duration(seconds=0),
                                          trigger = beam.trigger.AfterWatermark(),
                                          accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
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
            |"Add Processing Timestamp" >> beam.ParDo(AddProcessingTime()).with_output_types(NetLogAggSchema))

deadletters = (
    row.invalid 
    | "Batch Invalid Elements" >> beam.WindowInto(beam.window.FixedWindows(120),
                                                   trigger=beam.trigger.AfterProcessingTime(120),
                                                   accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
    | "Write Invalid Elements to BQ" >> beam.io.WriteToBigQuery(table="table_id",
                                                                schema="table_schema",
                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

predict_anomaly = (
    features
    | 'Make Predictions' >> beam.ParDo(PredictAnomaly("")).with_output_types(NetLogAggSchema)
    |  "write Anomaly to BQ" >> beam.io.WriteToBigQuery(table="table_id",
                                                        schema="table_schema",
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

write_to_log_table = deadletters = (
    row.valid 
    | "Batch Invalid Elements" >> beam.WindowInto(beam.window.FixedWindows(120),
                                                   trigger=beam.trigger.AfterProcessingTime(120),
                                                   accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
    | "Write Invalid Elements to BQ" >> beam.io.WriteToBigQuery(table="table_id",
                                                                schema="table_schema",
                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

write_to_feature_store = (
    features 
    |"Write Features to BQ" >>beam.io.WriteToBigQuery(table="table_id",                              
                                                    schema="table_schema",
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )




#   pcollection | WindowInto(
#     FixedWindows(1 * 60),
#     trigger=AfterProcessingTime(1 * 60),
#     accumulation_mode=AccumulationMode.DISCARDING)