FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/preprocessor ./src/preprocessor

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "preprocessor.main"]