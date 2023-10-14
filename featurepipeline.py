import json
import time
import typing
import google.auth
import apache_beam as beam
from datetime import datetime
from apache_beam.runners import DataflowRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions, SetupOptions


bucket = "gs://electric-armor-395015-netlog-bucket"
dataflow_gcs_location = f'{bucket}/dataflow'
files = f"{bucket}/*/*.json"
valid_out_path = f"{bucket}/netlog_aggregate/"
invalid_out_path = f"{bucket}/netlog_badletters/"
file_name_suffix = ".json"

    

class NetLogFeaturesSchema(typing.NamedTuple):
    subscriberId : str
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

class NetLogAggSchema(typing.NamedTuple):
    ProcessingTime : datetime
    subscriberId : str
    dstIP : str
    UniqueIPs : int
    UniquePorts : int
    NumRecords : int
    MinTxBytes: int
    MaxTxBytes: int
    AvgTxBytes: float
    MinRxBytes: int
    MaxRxBytes: int
    AvgRxBytes: float
    MinDuration: float
    MaxDuration: float
    AvgDuration: float

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
            event = json.loads(element)#.decode('utf-8')
            yield beam.pvalue.TaggedOutput('validjson', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalidjson', element) #.decode('utf-8'))

class AssignTimeStamp(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S.%f"
        timestamp = datetime.strptime(element.endTime, timefmt).timestamp()
        yield beam.window.TimestampedValue(element, timestamp)
        
class AddProcessingTime(beam.DoFn):
    def process(self, element):
        element = element._asdict()
        element['ProcessingTime'] = datetime.now().isoformat()
        yield dict(element)
            
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


pipeline = beam.Pipeline()

json_rows = (
    pipeline 
    |"Read From Text" >> beam.io.ReadFromText(files)
    |"Parse Event" >> beam.ParDo(EventParser()).with_outputs('validjson', 'invalidjson').with_output_types(NetLogRawSchema)
    )


rows = (
    json_rows.validjson 
    |"Convert To Row" >> beam.ParDo(JsonToBeamRow()).with_outputs('validrow', 'invalidrow').with_output_types(NetLogRowSchema)
    ) 


aggregate_data = (
    rows.validrow
    | 'With timestamps' >> beam.ParDo(AssignTimeStamp())
    | "Fixed Window 1 Min" >> beam.WindowInto(beam.window.FixedWindows(120),
                                  allowed_lateness=beam.window.Duration(seconds=0),
                                  # trigger = eam.trigger.Repeatedly(
                                  #      AfterAny(AfterCount(10), AfterProcessingTime(1 * 60))),
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
    |"Add Processing Timestamp" >> beam.ParDo(AddProcessingTime()).with_output_types(NetLogAggSchema)
    | "Write Features To Cloud Storage" >> beam.io.WriteToText(file_path_prefix=valid_out_path,file_name_suffix=file_name_suffix)
    )

badletters =  (
    (json_rows.invalidjson , rows.invalidrow) 
       | "Flatten" >> beam.Flatten() 
       | 'Deduplicate elements' >> beam.Distinct()
       | "Write Bad Letter To Cloud Storage" >>beam.io.WriteToText(file_path_prefix=invalid_out_path,file_name_suffix=file_name_suffix)
    )

if __name__ == "__main__":
    runner = DataflowRunner()
    options = pipeline_options.PipelineOptions()
    _, options.view_as(GoogleCloudOptions).project = google.auth.default()
    options.view_as(GoogleCloudOptions).region = 'europe-west1'
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(GoogleCloudOptions).job_name = 'Netlog Model Test Job' + str(datetime.now())
    options.view_as(GoogleCloudOptions).staging_location = f'{dataflow_gcs_location}/staging'
    options.view_as(GoogleCloudOptions).temp_location = f'{dataflow_gcs_location}/temp'
    runner.run_pipeline(pipeline, options=options)
    
#  class NetLogFeaturesSchema(typing.NamedTuple):
#     subscriberId : str
#     Records : int
#     MinTxBytes: int
#     MaxTxBytes: int
#     AvgTxBytes: float
#     MinRxBytes: int
#     MaxRxBytes: int
#     AvgRxBytes: float
#     MinDuration: float
#     MaxDuration: float
#     AvgDuration: float

# class NetLogRawSchema(typing.NamedTuple):
#     subscriberId : str
#     srcIP : str
#     dstIP : str
#     srcPort : int
#     dstPort : int
#     txBytes : int
#     rxBytes : int
#     startTime : datetime
#     endTime : datetime
#     tcpFlag : str
#     protocolName : str
#     protocolNumber : int

# class NetLogAggSchema(typing.NamedTuple):
#     ProcessingTime : datetime
#     subscriberId : str
#     dstIP : str
#     UniqueIPs : int
#     UniquePorts : int
#     NumRecords : int
#     MinTxBytes: int
#     MaxTxBytes: int
#     AvgTxBytes: float
#     MinRxBytes: int
#     MaxRxBytes: int
#     AvgRxBytes: float
#     MinDuration: float
#     MaxDuration: float
#     AvgDuration: float

# class NetLogRowSchema(typing.NamedTuple):
#     subscriberId: str
#     srcIP: str
#     srcPort: int
#     dstIP: str
#     dstPort: int
#     txBytes: int
#     rxBytes: int
#     startTime: datetime
#     endTime: datetime
#     duration: float
    
# class UniqueCombine(beam.CombineFn):
#     def create_accumulator(self):
#         return set()
    
#     def add_input(self, accumulator, element):
#         accumulator.add(element)
#         return accumulator
    
#     def merge_accumulators(self, accumulators):
#         return set.union(*accumulators)
    
#     def extract_output(self, accumulator):
#         return len(accumulator)

# class EventParser(beam.DoFn):
#     def process(self, element):
#         try:
#             event = json.loads(element)
#             yield beam.pvalue.TaggedOutput('validjson', NetLogRawSchema(**event))
#         except:
#             yield beam.pvalue.TaggedOutput('invalidjson', element)

# class AssignTimeStamp(beam.DoFn):
#     def process(self, element):
#         timefmt = "%Y-%m-%dT%H:%M:%S.%f"
#         timestamp = datetime.strptime(element.endTime, timefmt).timestamp()
#         yield beam.window.TimestampedValue(element, timestamp)
        
# class AddProcessingTime(beam.DoFn):
#     def process(self, element):
#         element = element._asdict()
#         element['ProcessingTime'] = datetime.now().isoformat()
#         yield dict(element)
            
# class JsonToBeamRow(beam.DoFn):
#     def process(self, element):
#         try:
#             timefmt = "%Y-%m-%dT%H:%M:%S.%f"
#             duration = datetime.strptime(element.endTime, timefmt) - datetime.strptime(element.startTime, timefmt)
#             row = beam.Row(subscriberId=element.subscriberId,
#                             srcIP=element.srcIP,
#                             srcPort=element.srcPort,
#                             dstIP=element.dstIP,
#                             dstPort=element.dstPort,
#                             txBytes=element.txBytes,
#                             rxBytes=element.rxBytes,
#                             startTime=element.startTime,
#                             endTime=element.endTime,
#                             duration=duration.total_seconds())
#             yield beam.pvalue.TaggedOutput('validrow', row)
#         except:
#             yield beam.pvalue.TaggedOutput('invalidrow', element)


# def netlog_feature_aggregation(bucket="gs://electric-armor-395015-netlog-bucket"):   
#     import json
#     import time
#     import typing
#     import google.auth
#     import apache_beam as beam
#     from datetime import datetime
#     from apache_beam.runners import DataflowRunner
#     from apache_beam.options import pipeline_options
#     from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions, SetupOptions
    
#     files = f"{bucket}/*/*.json"
#     valid_out_path = f"{bucket}/netlog_aggregate/"
#     invalid_out_path = f"{bucket}/netlog_badletters/"
#     file_name_suffix = ".json"

#     runner = DataflowRunner()
#     options = pipeline_options.PipelineOptions()
#     _, options.view_as(GoogleCloudOptions).project = google.auth.default()
#     options.view_as(SetupOptions).save_main_session = False
#     options.view_as(GoogleCloudOptions).region = 'europe-west1'
#     options.view_as(GoogleCloudOptions).job_name = 'Netlog Model Test Job' + str(datetime.now())
#     options.view_as(GoogleCloudOptions).staging_location = f'{bucket}/staging'
#     options.view_as(GoogleCloudOptions).temp_location = f'{bucket}/temp'


#     @beam.ptransform_fn
#     def GetFeaturesFromRow(pcoll):
#         return (
#             pcoll
#             |"With timestamps" >> beam.ParDo(AssignTimeStamp())
#             | "Fixed Window 1 Min" >> beam.WindowInto(beam.window.FixedWindows(120),
#                               allowed_lateness=beam.window.Duration(seconds=0),
#                               trigger = beam.trigger.AfterWatermark(),
#                               accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
#             | "Aggregate Row" >> beam.GroupBy("subscriberId","dstIP")
#                         .aggregate_field("srcIP", UniqueCombine(), "UniqueIPs")
#                         .aggregate_field("srcPort", UniqueCombine(), "UniquePorts")
#                         .aggregate_field("subscriberId", beam.combiners.CountCombineFn(),"Records")
#                         .aggregate_field("txBytes", min ,"MinTxBytes")
#                         .aggregate_field("txBytes", max ,"MaxTxBytes")
#                         .aggregate_field("txBytes", beam.combiners.MeanCombineFn() ,"AvgTxBytes")
#                         .aggregate_field("rxBytes", min,"MinRxBytes")
#                         .aggregate_field("rxBytes", max,"MaxRxBytes")
#                         .aggregate_field("rxBytes", beam.combiners.MeanCombineFn() ,"AvgRxBytes")
#                         .aggregate_field("duration", min, "MinDuration")
#                         .aggregate_field("duration", max, "MaxDuration")
#                         .aggregate_field("duration", beam.combiners.MeanCombineFn(), "AvgDuration")
#                )    
            
#     with beam.Pipeline() as pipeline:
#         json_rows = (
#             pipeline
#             |"Read From Text" >> beam.io.ReadFromText(files)
#             | "Parse Event" >> beam.ParDo(EventParser()).with_outputs('validjson', 'invalidjson').with_output_types(NetLogRawSchema)
#         )
        
#         beam_rows = (
#             json_rows.validjson
#             | "Convert To Row" >> beam.ParDo(JsonToBeamRow()).with_outputs('validrow', 'invalidrow').with_output_types(NetLogRowSchema)
#         )
            
        
#         features = (
#             beam_rows.validrow
#             | "Get Features" >> GetFeaturesFromRow()
#             | "Add Processing Timestamp" >> beam.ParDo(AddProcessingTime()).with_output_types(NetLogAggSchema)
#             | "Write Features To Cloud Storage" >> beam.io.WriteToText(file_path_prefix=valid_out_path,file_name_suffix=file_name_suffix)
#         )
        
#         badletters =  (
#             (json_rows.invalidjson , beam_rows.invalidrow)
#             | "Flatten" >> beam.Flatten()
#             | 'Deduplicate elements' >> beam.Distinct()
#             | "Write Bad Letter To Cloud Storage" >>beam.io.WriteToText(file_path_prefix=invalid_out_path,file_name_suffix=file_name_suffix)
#         )
#     runner.run_pipeline(pipeline, options=options)

# netlog_feature_aggregation()