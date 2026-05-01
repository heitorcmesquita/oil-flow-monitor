# Oil Flow Monitor — Phase 1 (AIS ingestion) — Minimal MVP

This repository implements the minimal Phase 1 ingestion pipeline:

- `producer/ais_producer.py` — async WebSocket client to AISStream that forwards raw JSON messages to Kafka topic `ais.raw`.
- `consumer/parquet_writer.py` — Kafka consumer that batches raw messages and writes compressed Parquet files partitioned by ingestion `year/month/day/hour` under a mounted `data/` directory.

Goal: keep the MVP small and runnable from the terminal. A single Docker image builds the Python runtime and application code; run it as either `producer` or `consumer` using the `SERVICE` environment variable.

Prerequisites
- Docker (to run Kafka and build the app image)
- (Optional) Python 3.12 if you want to run components locally instead of in Docker
- AISStream API key (set `AISSTREAM_API_KEY` in your environment or in `.env`)

Quick run (recommended)

1. Start Kafka (Zookeeper + Kafka) in Docker (single-host test):

```powershell
# create network
docker network create kafka-net || true

# start zookeeper
docker run -d --name zookeeper --network kafka-net -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:7.6.0

# start kafka
docker run -d --name kafka --network kafka-net -p 9092:9092 -p 29092:29092 \
	-e KAFKA_BROKER_ID=1 \
	-e KAFKA_ZOOKEEPER_CONNECT='zookeeper:2181' \
	-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT' \
	-e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092' \
	-e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092' \
	-e KAFKA_INTER_BROKER_LISTENER_NAME='PLAINTEXT_INTERNAL' \
	-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
	confluentinc/cp-kafka:7.6.0

# Allow Kafka to become ready (give it ~10-15s)
sleep 15

# create topic ais.raw
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic ais.raw --partitions 3 --replication-factor 1 --if-not-exists
```

2. Build the single app image

```bash
docker build -t oilflow:latest .
```

3. Start the consumer (writes Parquet to host `./data`)

```bash
mkdir -p data
docker run -d --name oilflow-consumer --network kafka-net -v "$(pwd)/data:/app/data" -e SERVICE=consumer -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 oilflow:latest
```

4. Start the producer (connects to AISStream)

```bash
# ensure AISSTREAM_API_KEY is set in your environment
docker run -d --name oilflow-producer --network kafka-net -e SERVICE=producer -e AISSTREAM_API_KEY="$AISSTREAM_API_KEY" -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 oilflow:latest
```

5. Verify Parquet files

On the host, list recent parquet files written by the consumer:

```bash
ls -R data | grep \.parquet || true

# Or on PowerShell
Get-ChildItem .\data -Recurse -Filter '*.parquet' | Sort-Object LastWriteTime -Descending | Select-Object -First 10
```

6. Quick sample (inspect one parquet file with pandas on host)

Install requirements on host and run a one-liner:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python - <<'PY'
import pandas as pd, glob
files = glob.glob('data/**/*.parquet', recursive=True)
print('found', len(files))
print(pd.read_parquet(files[0]).head())
PY
```

Notes
- The single image contains both `producer` and `consumer` code. Use `SERVICE` to choose which to run.
- The consumer writes raw JSON strings in a `raw` column with an `ingestion_ts` column appended. Files are partitioned by ingestion date/hour inside the mounted `data/` directory.
- This repository now contains the minimal MVP only: `producer/ais_producer.py`, `consumer/parquet_writer.py`, a consolidated `requirements.txt`, and a single `Dockerfile`.

If you want, I can now:
- add systemd/Windows service wrappers to start the producer and consumer containers on boot, or
- provide a small monitoring/healthcheck endpoint for the consumer container.
Which would you prefer?
