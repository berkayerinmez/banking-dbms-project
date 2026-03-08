FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary requests faker

COPY generate.py /app/generate.py

CMD ["python", "/app/generate.py"]
