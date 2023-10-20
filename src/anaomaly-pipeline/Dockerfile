FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

ARG WORKDIR="/dataflow/template"
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
COPY . ${WORKDIR}/

RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip 
    
RUN python setup.py install
ENV PIP_NO_DEPS=True