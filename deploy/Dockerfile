FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY teamfish/ ./teamfish/
RUN pip install --no-cache-dir .

COPY config/ ./config/
COPY jobs/ ./jobs/

ENV PYTHONPATH=/app

RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid 1001 --no-create-home appuser
USER appuser

CMD ["python", "jobs/run_daily.py"]
