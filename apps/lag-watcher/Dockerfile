FROM python:3.11-slim

WORKDIR /app
RUN apt-get update && apt-get install -y jq bash coreutils && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY watcher.py .
CMD ["python", "watcher.py"]
