FROM  python:3.11-slim

RUN pip install --upgrade pip

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY ./app .

CMD ["python3", "stream_event_generator.py" ]