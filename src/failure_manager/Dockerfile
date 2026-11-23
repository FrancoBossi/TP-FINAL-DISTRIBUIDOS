FROM python:3.12-slim

WORKDIR /app

COPY src/common ./src/common
COPY src/failure_manager ./src/failure_manager

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "failure_manager.main"]
