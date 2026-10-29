FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/infra ./src/infra
COPY src/q2/__init__.py ./src/q2/__init__.py
COPY src/q2/image_fetcher ./src/q2/image_fetcher

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "q2.image_fetcher.main"]