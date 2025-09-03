FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1
WORKDIR /app

# Install deps (force the correct kafka client)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
 && pip uninstall -y kafka || true \
 && pip install --no-cache-dir -r /app/requirements.txt

# Copy the whole apps/ tree so relative imports work
COPY apps /app/apps
ENV PYTHONPATH=/app

# Run as a package module to allow relative imports
CMD ["python", "-m", "apps.ingest.rss_worker"]
