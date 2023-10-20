import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.custom import NetLogRawSchema, NetLogRowSchema
from utils.custom import UniqueCombine, EventParser, JsonToBeamRow, AddProcessingTime

beam.coders.registry.register_coder(NetLogRawSchema, beam.coders.RowCoder)
beam.coders.registry.register_coder(NetLogRowSchema, beam.coders.RowCoder)


netlogbqschema = "subscriberId:STRING,srcIP:STRING,dstIP:STRING,srcPort:INTEGER,dstPort:INTEGER,txBytes:INTEGER,rxBytes:INTEGER,startTime:DATETIME,endTime:DATETIME,tcpFlag:STRING,protocolName:STRING,protocolNumber:INTEGER"
    
class GetFeaturesFromRow(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
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

def run(args, beam_args):   
    valid_out_path = args.bucket + "/aggregate/"
    invalid_out_path = args.bucket + "/badletters/"
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline =  beam.Pipeline(options=options)
    
    json_row = (
        pipeline
        |"Read From Text" >> beam.io.ReadFromPubSub(topic=args.topic)
        | "Parse Event" >> beam.ParDo(EventParser()).with_outputs('valid', 'invalid').with_output_types(NetLogRawSchema)
    )
    
    raw = (
        json_row.valid
        | "RawFixedWindow2MinWindow" >> beam.WindowInto(beam.window.FixedWindows(120),
                                        allowed_lateness=beam.window.Duration(seconds=0),
                                        trigger = beam.trigger.AfterWatermark(),
                                        accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
        | "NetLogRawToDict" >> beam.Map(lambda x: x._asdict())
        | "WriteRawLogsToBigquery" >> beam.io.WriteToBigQuery(table=args.netlog_bq_table,
                                                    schema=netlogbqschema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)  
        )
    
    features = (
        json_row.valid
        | "Convert To Row" >> beam.ParDo(JsonToBeamRow()).with_output_types(NetLogRowSchema)
        | "FeaturesFixed2MinWindow" >> beam.WindowInto(beam.window.FixedWindows(120),
                                            allowed_lateness=beam.window.Duration(seconds=0),
                                            trigger = beam.trigger.AfterWatermark(),
                                            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
        | "Get Features" >> GetFeaturesFromRow()
        | "Write Features To Cloud Storage" >> beam.io.WriteToText(file_path_prefix=valid_out_path,file_name_suffix=args.file_name_suffix)
    )

    badletters =  (
        json_row.invalid
        | "Write Bad Json To Cloud Storage" >>beam.io.WriteToText(file_path_prefix=invalid_out_path,file_name_suffix=args.file_name_suffix)
    )

    return pipeline.run()