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
    publishTime: datetime
    startTime : datetime
    endTime : datetime
    tcpFlag : str
    protocolName : str
    protocolNumber : int

class NetLogAggSchema(typing.NamedTuple):
    subscriberId : str
    dstIP : str
    UniqueIPs : int
    UniquePorts : int
    NumRecords : int
    MinTxBytes : int
    MaxTxBytes: int
    AvgTxBytes : float
    MinRxBytes : int
    MaxRxBytes: int
    AvgRxBytes : float
    MinDuration : int
    MaxDuration: int
    AvgDuration : float
    

class NetLogRowSchema(typing.NamedTuple):
    subscriberId: str
    srcIP: str
    srcPort: int
    dstIP: str
    dstPort: int
    txBytes: int
    rxBytes: int
    publishTime: datetime
    startTime: datetime
    endTime: datetime
    duration: float
    tcpFlag : str
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
        return len(accumulator)

class EventParser(beam.DoFn):
    """
    This Dofn parses the input from the pubsub topic to a json object and adds a tag to indicate if its a valid json or not.
    """
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        try:
            event = json.loads(element.decode('utf-8'))
            event['publishTime'] = publish_time
            yield beam.pvalue.TaggedOutput('valid', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalid', element.decode('utf-8'))

        
class AddProcessingTime(beam.DoFn):
    """
    Add a processing time field to each element
    """
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
        element.ProcessingTime = datetime.now().strftime(timefmt)
        yield element
            
class JsonToBeamRow(beam.DoFn):
    """
    This Dofn converts each element in the Pcollection to a Beam Row Object.
    """
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
        publish_time = datetime.utcfromtimestamp(float(publish_time)).strftime(timefmt)
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
        )