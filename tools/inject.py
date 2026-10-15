"""Erzeugt gezielt Nachrichten für reproduzierbare Resilienztests.

Aufruf aus dem Projektordner:
    python -m tools.inject invalid-json      # syntaktisch ungültig        -> DLQ (rejected)
    python -m tools.inject missing-field     # Pflichtfeld fehlt           -> DLQ (rejected)
    python -m tools.inject bad-value         # value = "heiß"              -> DLQ (rejected)
    python -m tools.inject poison            # gültig, aber poison=true    -> 3x NACK -> DLQ (delivery_limit)
    python -m tools.inject valid --count 20  # gültige Messwerte (z. B. für TTL-Test)
    python -m tools.inject alarm-burst       # 10 WARNING + 3 CRITICAL direkt an "alarms" (Prioritätstest)
"""

import argparse
import json
import uuid
from datetime import datetime, timezone

from common import config
from common.rabbit import ReliablePublisher

ROUTING_KEY = "sensor.temperature.inject-01"


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def measurement(value=55.0, **extra):
    return {"sensor_id": "inject-01", "timestamp": now(), "type": "temperature", "value": value, **extra}


def alarm(severity, i):
    return {
        "alarm_id": str(uuid.uuid4()), "severity": severity, "sensor_id": f"burst-{i:02d}",
        "type": "temperature", "value": 90.0 if severity == "WARNING" else 120.0,
        "threshold": config.THRESHOLDS["temperature"][0 if severity == "WARNING" else 1],
        "unit": "°C", "measured_at": now(), "detected_at": now(), "source": "inject",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("case", choices=["invalid-json", "missing-field", "bad-value", "poison", "valid", "alarm-burst"])
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()

    pub = ReliablePublisher("inject")
    try:
        if args.case == "alarm-burst":
            for i in range(10):
                pub.publish(config.EX_ALARMS, "warning", alarm("WARNING", i), priority=config.PRIORITY["WARNING"])
            for i in range(10, 13):
                pub.publish(config.EX_ALARMS, "critical", alarm("CRITICAL", i), priority=config.PRIORITY["CRITICAL"])
            print("10 WARNING + 3 CRITICAL an 'alarms' gesendet (CRITICAL zuletzt).")
            return

        for i in range(args.count):
            if args.case == "invalid-json":
                body = '{"sensor_id": "inject-01", "value": 42,'  # abgeschnittenes JSON
            elif args.case == "missing-field":
                body = json.dumps({"sensor_id": "inject-01", "type": "temperature", "value": 42})
            elif args.case == "bad-value":
                body = json.dumps(measurement(value="heiß"))
            elif args.case == "poison":
                body = json.dumps(measurement(poison=True))
            else:
                body = json.dumps(measurement(value=50.0 + i))
            pub.publish(config.EX_SENSOR, ROUTING_KEY, body)
            print(f"[{args.case}] -> {ROUTING_KEY}: {body}")
    finally:
        pub.close()


if __name__ == "__main__":
    main()
