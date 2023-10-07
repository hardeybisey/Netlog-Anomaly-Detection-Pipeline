## Realtime Anomaly Detection Using Cloud Stream Analytics and Vertex AI.

# Background
Due to the increasing threats and attacks on network infrastructure, it has become essential for companies to closely monitor the traffic flowing through their network systems. This monitoring allows them to promptly identify unusual network activity, often caused by malicious actors. This project addresses this concern by developing a solution that continuously monitors network traffic in nearly real-time and triggers alerts whenever any abnormal behavior is detected.


## NetLog Data Description
- **subscriberId:** This field represent the identifier of the subscriber or user associated with the network activity. 
- **srcIP:** The `srcIP` address represents the sender's IP address. 
- **dstIP:** The `dstIP` address indicates where the network traffic is being sent.
- **srcPort:** `srcPort` refers to the port number used on the sender's side of the connection. Port numbers can help identify the application or service associated with the traffic.
- **dstPort:** `dstPort` is the port number used on the receiver's side of the connection. It's important for determining the service or application receiving the traffic.
- **txBytes:** This field indicates the number of bytes transmitted from the source to the destination. Unusual spikes in data volume can be indicative of anomalies.
- **rxBytes:** It represents the number of bytes received by the destination. Similar to `txBytes`, monitoring `rxBytes` helps identify unusual data patterns.
- **tcpFlag:** `tcpFlag` provide information about the state of a TCP connection (e.g., SYN, ACK, FIN). Anomalies in flag patterns may indicate suspicious behavior.
- **startTime:** The `startTime` indicates when the connection or network activity started.
- **endTime:** The `endTime` indicates when the connection or activity ended. Duration of connections can be analyzed for anomalies.
- **timestamp:** A `timestamp` indicating when the log entry was recorded.
- **protocolName:** This field specifies the name of the protocol used for the network connection (e.g., HTTP, UDP, TCP). Anomalies can be detected by monitoring protocol usage.
- **protocolNumber:** This might represent the protocol as a numeric identifier (e.g., 6 for TCP, 17 for UDP). It can be used to classify connections based on protocols.
- **geoCountry:** The country associated with the `srcIP`. Geo-location data can help identify suspicious traffic from unexpected locations.
- **geoCity:** The city associated with the `srcIP`. Like geoCountry, this can be used to track traffic from specific geographical areas.
- **latitude:** These fields provide precise geographical coordinates associated with the `srcIP`. They can be used for mapping the locations of network activity and detecting unusual patterns geographically.
- **longitud**e: These fields provide precise geographical coordinates associated with the `srcIP`. They can be used for mapping the locations of network activity and detecting unusual patterns geographically.

## Technologies
- BigQuery
- Cloud Storage
- Pub/Sub
- DataFlow
- Cloud Build
- Vertex AI

## Steps :
- Historical Data Ingestion
- Realtime Data Ingestion
- Data Normalization
- Feature Store 
- Model Development
- Model Serving in real Time
- Model Retraining for Drift

# Generating Mock Data
To ensure the creation of consistent and meaningful mock data, we implemented specific techniques:

1. **Time Sequencing:** We establish a logical sequence in our data by guaranteeing that the end time of an event always occurs after the start time. The time intervals between events are generated from a uniform distribution with an adjustable parameter known as `average_time_between_requests` (measured in seconds). This parameter allows us to control the time gaps between events, introducing a sense of order into the data.

2. **Btyes Size Per Request:** Additionally, the value of the time gap, determined by `average_time_between_requests`, plays a dual role in controlling the size of the `txBytes` (bytes transmitted) and `rxBytes` (bytes received) fields. By manipulating this parameter, we can influence the data volume associated with each event. This approach enables us to create data that mimics various scenarios, from low traffic to high-volume data transfers, providing a more comprehensive testing environment.

3. **Geolocation Attributes:** To enhance the realism of the data generated, the Value of the `srcIP` is used to derive related attributes, including `geoCountry`, `geoCity`, `latitude`, and `longitude`. This data synergy enriches the mock data's context.

4. **Destination IP:** We recognize the need for determinism in generating the `dstIP`. This attribute's deterministic nature is vital because it plays a vital role during aggregation of the events. Hence, we've implemented a custom generator to ensure consistency and reliability in the data simulation.
<!-- These techniques not only eliminate randomness but also empower you to simulate data that aligns with specific use cases and testing scenarios. Adjusting the `average_time_between_requests` parameter offers flexibility in shaping the characteristics of your mock data for more meaningful testing and analysis.

These strategies collectively eliminate randomness from the data generation process, imbuing your mock data with structure, relevance, and precision. This approach not only facilitates rigorous testing but also enables you to replicate a wide array of scenarios, fostering comprehensive evaluation and analysis. The flexibility provided by these techniques empowers you to tailor your mock data to specific use cases and testing objectives. -->

## Quick Start

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/GoogleCloudPlatform/df-ml-anomaly-detection.git)


## Clone The Repo
```
git clone https://github.com/hardeybisey/Netlog-Anomaly-Detection-Pipeline.git
```

### Enable APIs

```
gcloud auth configure-docker
gcloud services enable bigquery
gcloud services enable storage_component
gcloud services enable dataflow
gcloud services enable cloudbuild.googleapis.com
gcloud config set project <project_id>
```
### Access to Cloud Build Service Account 

```export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)") 
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role roles/editor
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role roles/storage.objectAdmin
```

#### Export Required Parameters 
```
export TOPIC_NAME="netlog-stream"
export SUBSCRIPTION_ID="netlog-stream-sub"
export DATASET_ID="netlog_dataset"
export BUCKET="$(gcloud config get-value project -q)-netlog-bucket"
export TAG="$(date +%Y%m%d%H%M%S)"
export EVENT_TYPE="normal"
```

#### Trigger Cloud Build Script

```
gcloud builds submit . --config cloud-build.yaml  --substitutions _TOPIC_NAME=${TOPIC_NAME},_SUBSCRIPTION_ID=${SUBSCRIPTION_ID},_DATASET_ID=${DATASET_ID},_BUCKET=${BUCKET},_TAG=${TAG},_EVENT_TYPE=${EVENT_TYPE}
```

