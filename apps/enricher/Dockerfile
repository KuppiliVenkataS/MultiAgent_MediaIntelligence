FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# Install only what the enricher needs
COPY apps/enricher/requirements.txt /app/requirements.txt
RUN pip uninstall -y kafka kafka-python || true \
 && pip install --no-cache-dir -r /app/requirements.txt \
 && python -m spacy download en_core_web_sm   # <-- install model here

# Copy code
COPY apps /app/apps
ENV PYTHONPATH=/app


# Show spaCy model to confirm at runtime (optional)
# RUN python -c "import spacy; nlp=spacy.load('en_core_web_sm'); print('spaCy model OK')"

CMD ["python", "-m", "apps.enricher.worker"]
