FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY driftwatch/ ./driftwatch/
COPY config/ ./config/
COPY jobs/ ./jobs/

ENV PYTHONPATH=/app

CMD ["python", "jobs/run_daily.py"]
