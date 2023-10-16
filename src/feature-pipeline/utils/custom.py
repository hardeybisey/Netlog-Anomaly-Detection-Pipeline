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
    startTime : datetime
    endTime : datetime
    tcpFlag : str
    protocolName : str
    protocolNumber : int


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
    """
    This Dofn parses the input from the pubsub topic to a json object and adds a tag to indicate if its a valid json or not.
    """
    def process(self, element):
        try:
            event = json.loads(element.decode('utf-8'))
            yield beam.pvalue.TaggedOutput('validj', NetLogRawSchema(**event))
        except:
            yield beam.pvalue.TaggedOutput('invalid', element.decode('utf-8'))


class AddProcessingTime(beam.DoFn):
    """
    Add a processing time field to each element
    """
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
        element = element._asdict()
        element['ProcessingTime'] = datetime.now().strftime(timefmt)
        yield element
            
class JsonToBeamRow(beam.DoFn):
    """
    This Dofn converts each element in the Pcollection to a Beam Row Object.
    """
    def process(self, element):
        timefmt = "%Y-%m-%dT%H:%M:%S"
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
                        duration=duration.total_seconds())
            )    
    
netlogbqschema = {
    "fields" : [
    {
      "mode": "REQUIRED",
      "name": "subscriberId",
      "type": "STRING"
    },
    {
      "name": "srcIP",
      "type": "STRING"
    },
    {
      "name": "dstIP",
      "type": "STRING"
    },
    {
      "name": "srcPort",
      "type": "INTEGER"
    },
    {
      "name": "dstPort",
      "type": "INTEGER"
    },
    {
      "name": "txBytes",
      "type": "INTEGER"
    },
    {
      "name": "rxBytes",
      "type": "INTEGER"
    },
    {
      "name": "startTime",
      "type": "DATETIME"
    },
    {
      "name": "endTime",
      "type": "DATETIME"
    },
    {
      "name": "tcpFlag",
      "type": "STRING"
    },
    {
      "name": "protocolName",
      "type": "STRING"
    },
    {
      "name": "protocolNumber",
      "type": "INTEGER"
    }
  ]
}
