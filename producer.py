"""Sensor-Producer: simuliert kontinuierlich Messwerte und publiziert sie an sensor.data.

Beispiele:
    python producer.py --type temperature --sensor-id temp-01
    python producer.py --type all --interval 0.5
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone

from pika.exceptions import AMQPError

from common import config
from common.metrics import Metrics
from common.rabbit import ReliablePublisher, setup_logging

log = logging.getLogger("producer")

# Normalbereich (Mittelwert, Streuung) und Ausreißerbereich je Sensortyp
PROFILES = {
    "temperature": {"mean": 60.0, "sd": 8.0, "spike": (82.0, 110.0)},
    "humidity": {"mean": 50.0, "sd": 10.0, "spike": (86.0, 99.0)},
    "vibration": {"mean": 3.0, "sd": 1.5, "spike": (7.5, 13.0)},
}


def simulate(sensor_type, rng, anomaly_rate):
    p = PROFILES[sensor_type]
    if rng.random() < anomaly_rate:
        value = rng.uniform(*p["spike"])
    else:
        value = rng.gauss(p["mean"], p["sd"])
    if sensor_type == "humidity":
        value = min(max(value, 0.0), 100.0)
    if sensor_type == "vibration":
        value = max(value, 0.0)
    return round(value, 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--type", choices=[*config.SENSOR_TYPES, "all"], default="all")
    parser.add_argument("--sensor-id", help="Standard: <typ>-01")
    parser.add_argument("--interval", type=float, default=1.0, help="Sekunden zwischen Messungen")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Anteil simulierter Ausreißer")
    parser.add_argument("--count", type=int, default=0, help="Anzahl Messungen je Sensor (0 = endlos)")
    parser.add_argument("--seed", type=int, help="Zufalls-Seed für reproduzierbare Werte")
    args = parser.parse_args()

    types = list(config.SENSOR_TYPES) if args.type == "all" else [args.type]
    sensors = [(t, args.sensor_id or f"{t[:4]}-01") for t in types]
    setup_logging(f"producer:{args.type}")
    rng = random.Random(args.seed)
    publisher = ReliablePublisher(f"producer-{args.type}")

    sent = 0
    try:
        while args.count == 0 or sent < args.count * len(sensors):
            for sensor_type, sensor_id in sensors:
                message = {
                    "sensor_id": sensor_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "type": sensor_type,
                    "value": simulate(sensor_type, rng, args.anomaly_rate),
                    "unit": config.UNITS[sensor_type],
                }
                routing_key = f"sensor.{sensor_type}.{sensor_id}"
                retries = publisher.publish(config.EX_SENSOR, routing_key, message)
                sent += 1
                log.info("-> %s %s", routing_key, json.dumps(message, ensure_ascii=False))
                if retries:
                    Metrics(publisher.channel, f"producer-{args.type}").emit(
                        "publish_retry", retries=retries, sensor_type=sensor_type)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Beendet nach %d Nachrichten.", sent)
    except AMQPError as exc:
        log.error("Abbruch: %s (%d Nachrichten gesendet)", type(exc).__name__, sent)
        sys.exit(1)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
