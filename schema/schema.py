from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class NetLogRaw:
    subscriberId : str
    srcIP : str
    dstIP : str
    srcPort : int
    dstPort : int
    txBytes : int
    rxBytes : int
    tcpFlag : int
    startTime : datetime
    endTime : datetime
    protocolName : str
    protocolNumber : int

@dataclass
class NetLogOutlier:
    transaction_time: datetime
    subscriber_id: str
    dst_subnet: str
    number_of_unique_ips: int
    number_of_unique_ports: int
    number_of_records: int
    max_tx_bytes: int
    min_tx_bytes: int
    avg_tx_bytes: float
    max_rx_bytes: int
    min_rx_bytes: int
    avg_rx_bytes: float
    max_duration: int
    min_duration: int
    avg_duration: float
    centroid_id: int
 
@dataclass
class NetLogAggregate:
    transaction_time : datetime
    subscriber_id : str
    dst_subnet : str
    number_of_unique_ips : int
    number_of_unique_ports : int
    number_of_records : int
    max_tx_bytes : int
    min_tx_bytes : int
    avg_tx_bytes : float
    max_rx_bytes : int
    min_rx_bytes : int
    avg_rx_bytes : float
    max_duration : int
    min_duration : int
    avg_duration : float
    centroid_id : int