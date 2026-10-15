"""Alarm-Consumer: verarbeitet Alarme aus der Priority Queue q.alarms.

CRITICAL-Alarme (Priorität 9) werden vor WARNING-Alarmen (Priorität 5)
ausgeliefert. Damit die Priorität greift, ist prefetch_count=1: der Broker
sortiert nur Nachrichten, die noch nicht an den Consumer ausgeliefert wurden.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

from common import config
from common.metrics import Metrics
from common.rabbit import run_consumer, setup_logging

log = logging.getLogger("alarm")

REQUIRED = ("alarm_id", "severity", "sensor_id", "type", "value", "threshold", "detected_at")


def make_callback(args):
    def factory(connection):
        metrics = Metrics(connection.channel(), "alarm-consumer")

        def on_message(channel, method, properties, body):
            started = time.perf_counter()
            try:
                alarm = json.loads(body)
                missing = [f for f in REQUIRED if f not in alarm]
                if missing or alarm["severity"] not in config.PRIORITY:
                    raise ValueError(f"ungültiger Alarm (fehlend: {missing}, severity={alarm.get('severity')!r})")
            except (ValueError, TypeError) as exc:
                log.error("REJECT (-> DLQ): %s", exc)
                channel.basic_reject(method.delivery_tag, requeue=False)
                metrics.emit("failed", reason="invalid_alarm", detail=str(exc))
                return

            time.sleep(args.delay)  # simulierte Bearbeitung (z. B. Benachrichtigung)
            detected = datetime.fromisoformat(alarm["detected_at"])
            wait_s = (datetime.now(timezone.utc) - detected).total_seconds()
            text = (f"{alarm['type']} {alarm['sensor_id']} = {alarm['value']}{alarm.get('unit', '')} "
                    f"(Schwelle {alarm['threshold']}, prio={properties.priority}, wartete {wait_s:.1f}s)")
            if alarm["severity"] == "CRITICAL":
                log.critical("!!! CRITICAL - Eskalation an Bereitschaft: %s", text)
            else:
                log.warning("WARNING - protokolliert: %s", text)

            channel.basic_ack(method.delivery_tag)
            metrics.emit("alarm_handled", severity=alarm["severity"], sensor_type=alarm["type"],
                         processing_ms=round((time.perf_counter() - started) * 1000, 3))

        return on_message
    return factory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delay", type=float, default=0.2, help="simulierte Bearbeitungszeit je Alarm in s")
    args = parser.parse_args()
    setup_logging("alarm-consumer")
    run_consumer("alarm-consumer", config.Q_ALARMS, make_callback(args), prefetch=1)


if __name__ == "__main__":
    main()
