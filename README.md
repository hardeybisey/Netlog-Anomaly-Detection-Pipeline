## Realtime Anomaly Detection Using Cloud Stream Analytics and Vertex AI.

# Background
Due to the increasing threats and attacks on network infrastructure, it has become essential for companies to closely monitor the traffic flowing through their network systems. This monitoring allows them to promptly identify unusual network activity, often caused by malicious actors. This project addresses this concern by developing a solution that continuously monitors network traffic in nearly real-time and triggers alerts whenever any abnormal behavior is detected.

# Technologies
- BigQuery
- Cloud Storage
- Pub/Sub
- DataFlow
- Cloud Build
- Vertex AI

# Steps :
- Historical Data Ingestion
- Realtime Data Ingestion
- Data Normalization
- Feature Store 
- Model Development
- Model Serving in real Time
- Model Retraining for Drift

# NetLog Data Description
- subscriberId: This field represent the identifier of the subscriber or user associated with the network activity. 
<!-- Depending on your use case, this information may or may not be relevant for anomaly detection. It could be useful for tracking user-specific behavior. -->

- srcIP: The source IP address represents the sender's IP address. 
- dstIP: The destination IP address indicates where the network traffic is being sent.
- srcPort: Source port refers to the port number used on the sender's side of the connection. Port numbers can help identify the application or service associated with the traffic.
- dstPort: Destination port is the port number used on the receiver's side of the connection. It's important for determining the service or application receiving the traffic.
- txBytes: This field indicates the number of bytes transmitted from the source to the destination. Unusual spikes in data volume can be indicative of anomalies.
- rxBytes: It represents the number of bytes received by the destination. Similar to txBytes, monitoring rxBytes helps identify unusual data patterns.
- tcpFlag: TCP flags provide information about the state of a TCP connection (e.g., SYN, ACK, FIN). Anomalies in flag patterns may indicate suspicious behavior.
- startTime: The timestamp indicating when the connection or network activity started.
- endTime: The timestamp representing when the connection or activity ended. Duration of connections can be analyzed for anomalies.
- timestamp: A timestamp indicating when the log entry was recorded.
- protocolName: This field specifies the name of the protocol used for the network connection (e.g., HTTP, UDP, TCP). Anomalies can be detected by monitoring protocol usage.
- protocolNumber: This might represent the protocol as a numeric identifier (e.g., 6 for TCP, 17 for UDP). It can be used to classify connections based on protocols.
- geoCountry: The country associated with the IP address. Geo-location data can help identify suspicious traffic from unexpected locations.
- geoCity: The city associated with the IP address. Like geoCountry, this can be used to track traffic from specific geographical areas.
- latitude: These fields provide precise geographical coordinates. They can be used for mapping the locations of network activity and detecting unusual patterns geographically.
- longitude: These fields provide precise geographical coordinates. They can be used for mapping the locations of network activity and detecting unusual patterns geographically.
