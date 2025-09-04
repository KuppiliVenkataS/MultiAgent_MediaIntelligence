FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip uninstall -y kafka || true \
 && pip install --no-cache-dir -r /app/requirements.txt
COPY apps /app/apps
ENV PYTHONPATH=/app
CMD ["python", "-m", "apps.ingest.rss_worker"]
