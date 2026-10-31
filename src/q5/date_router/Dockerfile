FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY src/common ./src/common
COPY src/q5 ./src/q5

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "q5.date_router.main"]