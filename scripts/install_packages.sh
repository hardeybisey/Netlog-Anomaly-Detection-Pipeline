#/bin/sh
pip3 install 'apache-beam[gcp]==2.48.0'

python3 /Users/hardey/Desktop/GITHUB/AnomalyDetectionPipeline/test/data-pipeline/main.py --job_name=test --runner=DataflowRunner --project=electric-armor-395015 --region=europe-west2 --temp_location=gs://electric-armor-395015-netlog-bucket/tmp --sdk_container_image=hardeybisey/netlog-event-generator --topic=projects/electric-armor-395015/topics/netlog-topic --qps=1000 --event_type=anomaly --sdk_location=container --pickle_library=cloudpickle