FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pika nltk \
    && python -m nltk.downloader \
        -d /usr/local/share/nltk_data \
        stopwords

COPY src/common ./src/common
COPY src/q4_q5 ./src/q4_q5

ENV PYTHONPATH=/app/src
ENV NLTK_DATA=/usr/local/share/nltk_data

ENTRYPOINT ["python", "-m", "q4_q5.word_projector.main"]