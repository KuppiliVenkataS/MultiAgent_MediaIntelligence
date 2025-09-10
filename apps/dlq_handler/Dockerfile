FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backoff.py dlq_handler.py requeue_from_dlq.py ./
CMD ["python", "dlq_handler.py"]
