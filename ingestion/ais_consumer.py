#!/usr/bin/env python3

"""
consumer/parquet_writer.py

Consumes enriched tanker position telemetry from Kafka
and writes raw Bronze parquet files.

Topic:
    ais.positions

Output:
    data/bronze/*.parquet
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ais.positions"
)

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

DATA_ROOT = Path(
    os.getenv("DATA_ROOT", "./data")
)

BRONZE_DIR = DATA_ROOT / "bronze"

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "100")
)

FLUSH_INTERVAL_SECONDS = int(
    os.getenv(
        "FLUSH_INTERVAL_SECONDS",
        "120"
    )
)

GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "ais-positions-consumer"
)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(
    "parquet_writer"
)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def write_parquet(records):

    if not records:
        return

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.utcnow().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_file = (
        BRONZE_DIR
        / f"positions_{timestamp}.parquet"
    )

    df = pd.DataFrame(records)

    # Remove exact duplicates
    before = len(df)

    df = df.drop_duplicates()

    after = len(df)

    logger.info(
        "deduplicated rows_before=%s rows_after=%s removed=%s",
        before,
        after,
        before - after
    )

    table = pa.Table.from_pandas(df)

    pq.write_table(
        table,
        output_file,
        compression="snappy"
    )

    logger.info(
        "parquet-written rows=%s file=%s",
        len(df),
        output_file
    )


# ------------------------------------------------------------------------------
# Consumer
# ------------------------------------------------------------------------------


async def consume():

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,

        bootstrap_servers=(
            BOOTSTRAP_SERVERS
        ),

        group_id=GROUP_ID,

        auto_offset_reset="latest",

        enable_auto_commit=True,
    )

    await consumer.start()

    logger.info(
        "consumer-started"
    )

    batch = []

    last_flush = (
        asyncio.get_event_loop()
        .time()
    )

    try:

        while True:

            result = await consumer.getmany(
                timeout_ms=5000,
                max_records=5000
            )

            for tp, messages in result.items():

                for msg in messages:

                    try:

                        payload = json.loads(
                            msg.value.decode(
                                "utf-8"
                            )
                        )

                        batch.append(
                            payload
                        )

                    except Exception as exc:

                        logger.warning(
                            "bad-message error=%s",
                            exc
                        )

            now = (
                asyncio.get_event_loop()
                .time()
            )

            should_flush = (
                len(batch)
                >= BATCH_SIZE
                or
                (
                    now - last_flush
                )
                >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush and batch:

                logger.info(
                    "flushing-batch rows=%s",
                    len(batch)
                )

                write_parquet(batch)

                batch = []

                last_flush = now

    finally:

        if batch:

            write_parquet(batch)

        await consumer.stop()

        logger.info(
            "consumer-stopped"
        )


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------


if __name__ == "__main__":

    asyncio.run(consume())