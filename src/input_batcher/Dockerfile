FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/input_batcher ./src/input_batcher

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "input_batcher.main"]