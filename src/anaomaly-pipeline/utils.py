import typing
import datetime
import apache_beam as beam

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
    # srcIP : str
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
            # timeStamp=element['timeStamp'],
            # protocolName=element['protocolName'],
            # protocolNumber=element['protocolNumber'],
            duration=duration.total_seconds())