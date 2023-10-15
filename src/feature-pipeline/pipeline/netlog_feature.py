import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.custom import NetLogRawSchema, NetLogRowSchema
from utils.custom import UniqueCombine, EventParser, JsonToBeamRow, AddTimeStamp, AddProcessingTime

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

def run(opts, pipeline_opts):   
    valid_out_path = opts.bucket + "/aggregate/"
    invalid_out_path_row = opts.bucket + "/badletters/row/"
    invalid_out_path_json = opts.bucket + "/badletters/json/"
    options = PipelineOptions(pipeline_opts, save_main_session=True)
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