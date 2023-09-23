FROM  python:3.11-slim

RUN pip install --upgrade pip

RUN 
WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt --no-cache-dir

COPY . .

ENTRYPOINT ["python3", "batch_event_generator.py"]