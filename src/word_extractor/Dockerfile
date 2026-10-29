FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/word_extractor ./src/word_extractor

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "word_extractor.main"]
