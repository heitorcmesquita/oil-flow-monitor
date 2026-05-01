FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies from consolidated requirements file
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY producer/ /app/producer/
COPY consumer/ /app/consumer/

# Default data directory inside container (mount host dir here)
ENV DATA_ROOT=/app/data
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=ais.raw

# Entrypoint selects service via SERVICE env var (producer|consumer)
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

VOLUME ["/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
