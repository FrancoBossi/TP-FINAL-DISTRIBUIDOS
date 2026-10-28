FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/until_2020_filter ./src/until_2020_filter

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "until_2020_filter.main"]