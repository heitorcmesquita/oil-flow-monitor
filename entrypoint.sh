#!/usr/bin/env bash
set -euo pipefail

SERVICE=${SERVICE:-}

case "$SERVICE" in
  producer)
    echo "Starting AIS producer (will require AISSTREAM_API_KEY env var)"
    exec python /app/producer/ais_producer.py
    ;;
  consumer)
    echo "Starting Parquet consumer (writes to DATA_ROOT=${DATA_ROOT:-/app/data})"
    # Ensure DATA_ROOT env var propagates to consumer
    export DATA_ROOT=${DATA_ROOT:-/app/data}
    exec python /app/consumer/parquet_writer.py
    ;;
  *)
    echo "Usage: set SERVICE=producer|consumer and run this image. Example:"
    echo "  docker run --rm -e SERVICE=consumer -v \\$(pwd)/data:/app/data --network kafka-net oilflow:latest"
    exit 2
    ;;
esac
