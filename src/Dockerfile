FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR="/dataflow/template"
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

COPY . ${WORKDIR}/

RUN apt-get update \
    && apt-get install -y libpq-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install apache-beam[gcp]==2.48.0 \
    && pip install -r requirements.txt 

ENV PIP_NO_DEPS=true