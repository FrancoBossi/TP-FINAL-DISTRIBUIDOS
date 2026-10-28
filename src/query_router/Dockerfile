FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/query_router ./src/query_router

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "query_router.main"]