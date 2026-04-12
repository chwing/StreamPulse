FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required by confluent-kafka
# (it wraps librdkafka, a C library)

RUN apt-get update && apt-get install -y libpq-dev build-essential && rm -rf /var/lib/apt/lists/*


# Install Python dependencies first (Docker layer caching:
# this layer only rebuilds when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

CMD ["python3", "main.py"]