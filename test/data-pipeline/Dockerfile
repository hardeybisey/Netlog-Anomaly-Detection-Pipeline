FROM python:3.9

RUN pip install --no-cache-dir apache-beam[gcp]==2.48.0

COPY --from=apache/beam_python3.9_sdk:2.48.0 /opt/apache/beam /opt/apache/beam
COPY . .

RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip 
    
RUN python setup.py install

ENTRYPOINT [ "/opt/apache/beam/boot" ]

