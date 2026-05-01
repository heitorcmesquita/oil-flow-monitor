#!/usr/bin/env python3
"""
producer/ais_producer.py

Async AISStream -> Kafka producer.

Connects to AISStream websocket, subscribes to a bounding box around the
Strait of Hormuz, and forwards raw JSON messages into Kafka topic `ais.raw`.
"""
import asyncio
import json
import logging
import os
import signal
from typing import Optional

from aiokafka import AIOKafkaProducer
import websockets

# Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ais.raw")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
WS_URL = os.getenv("AISSTREAM_WS_URL", "wss://stream.aisstream.io/v0/stream")

BOUNDING_BOX = {
    "min_lat": float(os.getenv("AIS_MIN_LAT", 25)),
    "max_lat": float(os.getenv("AIS_MAX_LAT", 27)),
    "min_lon": float(os.getenv("AIS_MIN_LON", 56)),
    "max_lon": float(os.getenv("AIS_MAX_LON", 58)),
}

# Logging
logger = logging.getLogger("ais_producer")
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def send_to_kafka(producer: AIOKafkaProducer, topic: str, message: str, retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        try:
            await producer.send_and_wait(topic, message.encode("utf-8"))
            logger.debug("sent-to-kafka %s bytes", len(message))
            return True
        except Exception as exc:
            logger.warning("kafka-send-failed attempt=%d error=%s", attempt, exc)
            await asyncio.sleep(min(2 ** attempt, 30))
    logger.error("kafka-send-failed-final")
    return False


async def produce_loop(stop_event: asyncio.Event):
    if not AISSTREAM_API_KEY:
        logger.error("AISSTREAM_API_KEY is not set. Set it and restart.")
        return

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("kafka-producer-started", extra={"bootstrap_servers": BOOTSTRAP_SERVERS, "topic": KAFKA_TOPIC})

    backoff = 1
    async def _handle_ws(ws):
        logger.info("ws-connected", extra={"url": WS_URL})
        # Subscribe message - provider protocol may vary; this is a best-effort payload for bbox filtering
        subscribe = {
            "type": "subscribe",
            "filter": {
                "bbox": [
                    BOUNDING_BOX["min_lon"],
                    BOUNDING_BOX["min_lat"],
                    BOUNDING_BOX["max_lon"],
                    BOUNDING_BOX["max_lat"],
                ]
            },
            "api_key": AISSTREAM_API_KEY,
        }
        try:
            await ws.send(json.dumps(subscribe))
            logger.info("sent-subscribe", extra={"bbox": subscribe["filter"]["bbox"]})
        except Exception:
            logger.warning("subscribe-send-failed")

        nonlocal backoff
        backoff = 1
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=60)
                if not message:
                    continue
                # Forward raw message to Kafka
                await send_to_kafka(producer, KAFKA_TOPIC, message)
            except asyncio.TimeoutError:
                # keepalive
                continue
            except websockets.ConnectionClosed as cx:
                logger.warning("ws-closed %s %s", cx.code, cx.reason)
                break
            except Exception as exc:
                logger.exception("ws-read-error %s", exc)
                await asyncio.sleep(1)
                continue

    while not stop_event.is_set():
        try:
            # Set Authorization header (some streams accept it)
            headers = [("Authorization", f"Bearer {AISSTREAM_API_KEY}")]
            try:
                async with websockets.connect(WS_URL, extra_headers=headers, ping_interval=20, ping_timeout=20) as ws:
                    await _handle_ws(ws)
            except TypeError as exc:
                # Some websocket/asyncio combinations (or older/newer libs) may not accept extra_headers
                if "extra_headers" in str(exc) or "unexpected keyword" in str(exc):
                    logger.info("websockets.connect does not accept extra_headers; retrying without headers")
                    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                        await _handle_ws(ws)
                else:
                    raise
        except Exception as exc:
            logger.warning("ws-connect-error %s", exc)
        # exponential backoff before reconnect
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

    await producer.stop()
    logger.info("kafka-producer-stopped")


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event):
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: stop_event.set())
        loop.add_signal_handler(signal.SIGTERM, lambda: stop_event.set())
    except NotImplementedError:
        # Windows: add_signal_handler may not be implemented
        pass


def main():
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop, stop_event)
    try:
        loop.run_until_complete(produce_loop(stop_event))
    except KeyboardInterrupt:
        logger.info("keyboard-interrupt, stopping")
        stop_event.set()
    finally:
        if not loop.is_closed():
            loop.run_until_complete(asyncio.sleep(0.1))


if __name__ == "__main__":
    main()
