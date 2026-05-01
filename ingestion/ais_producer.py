#!/usr/bin/env python3

"""
producer/enriched_commercial_positions.py

Streams AIS PositionReport + ShipStaticData from AISStream,
builds an in-memory vessel registry,
enriches commercial vessel positions,
and publishes enriched telemetry to Kafka.

Commercial vessel classes:
70-89
- Cargo
- Tankers
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime

from aiokafka import AIOKafkaProducer
import websockets

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

AISSTREAM_API_KEY = os.getenv(
    "AISSTREAM_API_KEY"
)

WS_URL = "wss://stream.aisstream.io/v0/stream"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ais.positions"
)

POLL_INTERVAL_SECONDS = int(
    os.getenv(
        "POLL_INTERVAL_SECONDS",
        "120"
    )
)

# Commercial vessel classes
# 70-79 = Cargo
# 80-89 = Tankers
COMMERCIAL_TYPES = set(range(70, 90))

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(
    "enriched_commercial_positions"
)

# ------------------------------------------------------------------------------
# Vessel Registry
# ------------------------------------------------------------------------------

vessel_registry = {}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def normalize_destination(destination):

    if not destination:
        return ""

    return (
        str(destination)
        .upper()
        .replace(" ", "")
        .strip()
    )


def is_commercial(ship_type):

    try:
        return int(ship_type) in COMMERCIAL_TYPES

    except Exception:
        return False


def process_static_data(message):

    try:

        msg = (
            message["Message"]
            ["ShipStaticData"]
        )

        meta = message.get(
            "MetaData",
            {}
        )

        mmsi = meta.get("MMSI")

        if not mmsi:
            return

        vessel_registry[mmsi] = {

            "ship_name": meta.get(
                "ShipName"
            ),

            "imo": msg.get(
                "ImoNumber"
            ),

            "destination": (
                normalize_destination(
                    msg.get(
                        "Destination"
                    )
                )
            ),

            "ship_type": msg.get(
                "Type"
            ),

            "draught": msg.get(
                "MaximumStaticDraught"
            ),
        }

    except Exception as exc:

        logger.warning(
            "static-processing-error=%s",
            exc
        )


def extract_position_report(message):

    try:

        msg = (
            message["Message"]
            ["PositionReport"]
        )

        meta = message.get(
            "MetaData",
            {}
        )

        mmsi = meta.get("MMSI")

        if not mmsi:
            return None

        vessel = vessel_registry.get(
            mmsi
        )

        # Ignore vessels without metadata
        if not vessel:
            return None

        ship_type = vessel.get(
            "ship_type"
        )

        # Keep only commercial ships
        if not is_commercial(ship_type):
            return None

        record = {

            "event_time": (
                datetime.utcnow()
                .isoformat()
            ),

            "mmsi": mmsi,

            "ship_name": vessel.get(
                "ship_name"
            ),

            "imo": vessel.get(
                "imo"
            ),

            "destination": vessel.get(
                "destination"
            ),

            "ship_type": ship_type,

            "draught": vessel.get(
                "draught"
            ),

            "latitude": meta.get(
                "latitude"
            ),

            "longitude": meta.get(
                "longitude"
            ),

            "sog": msg.get(
                "Sog"
            ),

            "cog": msg.get(
                "Cog"
            ),

            "true_heading": msg.get(
                "TrueHeading"
            ),

            "nav_status": msg.get(
                "NavigationalStatus"
            ),
        }

        return record

    except Exception as exc:

        logger.warning(
            "position-processing-error=%s",
            exc
        )

        return None


async def flush_batch(
    producer,
    batch
):

    if not batch:
        return

    for record in batch:

        await producer.send_and_wait(
            KAFKA_TOPIC,
            json.dumps(record).encode("utf-8")
        )

    logger.info(
        "published-batch size=%s",
        len(batch)
    )


# ------------------------------------------------------------------------------
# Main Stream Loop
# ------------------------------------------------------------------------------


async def stream_loop():

    if not AISSTREAM_API_KEY:

        raise ValueError(
            "AISSTREAM_API_KEY not set"
        )

    producer = AIOKafkaProducer(
        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        ),
        enable_idempotence=True
    )

    await producer.start()

    logger.info(
        "kafka-producer-started"
    )

    subscription_message = {

        "APIKey": AISSTREAM_API_KEY,

        "BoundingBoxes": [
            [
                [-90, -180],
                [90, 180]
            ]
        ],

        "FilterMessageTypes": [
            "ShipStaticData",
            "PositionReport"
        ]
    }

    batch = []

    last_flush = (
        asyncio.get_event_loop()
        .time()
    )

    while True:

        try:

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None
            ) as websocket:

                logger.info(
                    "ws-connected"
                )

                await websocket.send(
                    json.dumps(
                        subscription_message
                    )
                )

                logger.info(
                    "subscription-sent"
                )

                while True:

                    raw_message = (
                        await asyncio.wait_for(
                            websocket.recv(),
                            timeout=60
                        )
                    )

                    message = json.loads(
                        raw_message
                    )

                    message_type = (
                        message.get(
                            "MessageType"
                        )
                    )

                    # ----------------------------------------------------------
                    # Static metadata
                    # ----------------------------------------------------------

                    if (
                        message_type
                        == "ShipStaticData"
                    ):

                        process_static_data(
                            message
                        )

                        continue

                    # ----------------------------------------------------------
                    # Position reports
                    # ----------------------------------------------------------

                    if (
                        message_type
                        == "PositionReport"
                    ):

                        record = (
                            extract_position_report(
                                message
                            )
                        )

                        if record:

                            batch.append(
                                record
                            )

                    now = (
                        asyncio
                        .get_event_loop()
                        .time()
                    )

                    should_flush = (
                        now - last_flush
                        >= POLL_INTERVAL_SECONDS
                    )

                    if should_flush and batch:

                        await flush_batch(
                            producer,
                            batch
                        )

                        batch = []

                        last_flush = now

        except asyncio.TimeoutError:

            logger.info(
                "ws-timeout"
            )

        except Exception as exc:

            logger.warning(
                "stream-error error=%s",
                exc
            )

            await asyncio.sleep(5)


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------


async def main():

    await stream_loop()


if __name__ == "__main__":

    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    for sig in (
        signal.SIGINT,
        signal.SIGTERM
    ):

        try:

            loop.add_signal_handler(
                sig,
                loop.stop
            )

        except NotImplementedError:
            pass

    try:

        loop.run_until_complete(
            main()
        )

    finally:

        loop.close()