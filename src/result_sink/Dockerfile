FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/result_sink ./src/result_sink

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "result_sink.main"]
