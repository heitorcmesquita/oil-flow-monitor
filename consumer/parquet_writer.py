#!/usr/bin/env python3
"""
consumer/parquet_writer.py

Consume `ais.raw` Kafka topic and write parquet files partitioned by ingestion date/hour.
Files are written under `data/raw/year=YYYY/month=MM/day=DD/hour=HH/`.
"""
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer

# Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ais.raw")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "parquet-writer-group")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "data/raw"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
BATCH_TIME_S = float(os.getenv("BATCH_TIME_S", "30"))

logger = logging.getLogger("parquet_writer")
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def write_parquet_batch(records):
    if not records:
        return
    ingestion_ts = datetime.now(timezone.utc)
    y = ingestion_ts.strftime("%Y")
    m = ingestion_ts.strftime("%m")
    d = ingestion_ts.strftime("%d")
    h = ingestion_ts.strftime("%H")
    dest = DATA_ROOT / f"year={y}" / f"month={m}" / f"day={d}" / f"hour={h}"
    dest.mkdir(parents=True, exist_ok=True)
    filename = f"batch-{int(time.time())}-{uuid.uuid4().hex[:8]}.parquet"
    path = dest / filename
    df = pd.DataFrame({"raw": records, "ingestion_ts": [ingestion_ts.isoformat()] * len(records)})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path.as_posix(), compression="gzip")
    logger.info("wrote-parquet", extra={"path": str(path), "records": len(records)})


async def consume_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    logger.info("consumer-started", extra={"topic": KAFKA_TOPIC})
    buffer = []
    last_flush = asyncio.get_event_loop().time()
    try:
        while True:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=BATCH_TIME_S)
                try:
                    raw = msg.value.decode("utf-8")
                except Exception:
                    raw = str(msg.value)
                buffer.append(raw)
                if len(buffer) >= BATCH_SIZE or (asyncio.get_event_loop().time() - last_flush) >= BATCH_TIME_S:
                    await write_parquet_batch(buffer)
                    await consumer.commit()
                    buffer.clear()
                    last_flush = asyncio.get_event_loop().time()
            except asyncio.TimeoutError:
                if buffer:
                    await write_parquet_batch(buffer)
                    await consumer.commit()
                    buffer.clear()
                    last_flush = asyncio.get_event_loop().time()
                continue
    finally:
        if buffer:
            await write_parquet_batch(buffer)
        await consumer.stop()
        logger.info("consumer-stopped")


def main():
    asyncio.run(consume_loop())


if __name__ == "__main__":
    main()
