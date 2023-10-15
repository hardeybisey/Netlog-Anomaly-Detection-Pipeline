import typing
import json
import apache_beam as beam
from datetime import datetime

class NetLogFeaturesSchema(typing.NamedTuple):
    subscriberId : str
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
            event = json.loads(element)
            yield beam.pvalue.TaggedOutput('validjson', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalidjson', element)

class AssignTimeStamp(beam.DoFn):
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S.%f"
        timestamp = datetime.strptime(element.endTime, timefmt).timestamp()
        yield beam.window.TimestampedValue(element, timestamp)
        
class AddProcessingTime(beam.DoFn):
    def process(self, element):
        element = element._asdict()
        element['ProcessingTime'] = datetime.now().isoformat()
        yield NetLogAggSchema(**element)
            
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
            yield beam.pvalue.TaggedOutput('invalidrow', element._asdict())