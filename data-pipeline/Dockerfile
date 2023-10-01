FROM apache/beam_python3.10_sdk:2.50.0

ARG WORKDIR="/dataflow/template"
# RUN mkdir -p ${WORKDIR}
# WORKDIR ${WORKDIR}

# ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
# ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
COPY . .

RUN apt-get update \
    # && apt-get install -y libpq-dev git \
    # && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip 
    
RUN python setup.py install
ENTRYPOINT [ "/opt/apache/beam/boot" ]


# python data-pipeline/main.py  --qps 1000 --topic 'projects/electric-armor-395015/topics/netlog-stream' --anomaly False --project electric-armor-395015 --region europe-west2 --temp_location "gs://electric-armor-395015-netlog-bucket/tmp" --staging_location "gs://electric-armor-395015-netlog-bucket/staging" --runner DataFlowRunner --prebuild_sdk_container_engine=cloud_build --docker_registory_push_url= "gcr.io/electric-armor-395015/netlog-dataflow-template:12"