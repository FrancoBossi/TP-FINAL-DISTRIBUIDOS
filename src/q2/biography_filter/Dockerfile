FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/q2/__init__.py ./src/q2/__init__.py
COPY src/q2/biography_filter ./src/q2/biography_filter

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "q2.biography_filter.main"]