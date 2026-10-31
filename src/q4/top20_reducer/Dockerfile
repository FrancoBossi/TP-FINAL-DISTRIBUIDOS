FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/q4 ./src/q4

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "q4.top20_reducer.main"]
