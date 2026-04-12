FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --default-timeout=100 -r /tmp/requirements.txt

WORKDIR /app
COPY . .

USER 1001

CMD ["python3", "/app/main.py"]