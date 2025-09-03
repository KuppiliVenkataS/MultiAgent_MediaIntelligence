FROM python:3.11-slim
WORKDIR /app
# use repo-root context so this file exists
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
# copy only the api app code into /app
COPY apps/api /app
EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
