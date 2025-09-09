FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY apps/graph_writer/requirements.txt /app/requirements.txt
RUN pip uninstall -y kafka kafka-python || true \
 && pip install --no-cache-dir -r /app/requirements.txt
COPY apps /app/apps
ENV PYTHONPATH=/app
CMD ["python", "-m", "apps.graph_writer.worker"]
