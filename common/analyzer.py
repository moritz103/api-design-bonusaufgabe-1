"""Gemeinsame Logik der Analyse-Consumer.

Ablauf je Nachricht:
    1. Validieren (JSON, Pflichtfelder, Wertebereich)   -> Fehler: REJECT ohne Requeue -> DLQ
    2. Anomalie prüfen, ggf. Alarm publizieren (Confirm) -> Fehler: REJECT mit Requeue
    3. Wert ins 10-s-Aggregationsfenster übernehmen
    4. ACK  (erst jetzt gilt die Nachricht als verarbeitet)

Ein REJECT mit Requeue führt zur erneuten Zustellung. Die Quorum Queue zählt
fehlgeschlagene Zustellungen (x-delivery-count) und leitet die Nachricht nach
Überschreiten von DELIVERY_LIMIT automatisch an den DLX weiter -> keine
Endlosschleife. Wichtig (seit RabbitMQ 4.3): nur basic.reject zählt als
"Verarbeitung fehlgeschlagen"; basic.nack erhöht den delivery-count NICHT
und würde daher endlos neu zugestellt.
"""

import argparse
import json
import logging
import math
import os
import statistics
import time
import uuid
from datetime import datetime, timezone

from . import config
from .metrics import Metrics
from .rabbit import json_properties, run_consumer, setup_logging

log = logging.getLogger("analyzer")

# Plausible physikalische Wertebereiche: alles außerhalb gilt als semantisch ungültig
VALID_RANGE = {
    "temperature": (-50.0, 200.0),
    "humidity": (0.0, 100.0),
    "vibration": (0.0, 100.0),
}


class InvalidMessage(Exception):
    """Permanenter Fehler: die Nachricht wird auch beim nächsten Versuch nicht gültig."""


class ProcessingError(Exception):
    """Temporärer Fehler: ein erneuter Versuch kann erfolgreich sein."""


def validate(body, accepted_types):
    try:
        msg = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidMessage(f"kein gültiges JSON: {exc}")
    if not isinstance(msg, dict):
        raise InvalidMessage("JSON-Objekt erwartet")
    missing = [f for f in ("sensor_id", "timestamp", "type", "value") if f not in msg]
    if missing:
        raise InvalidMessage(f"Pflichtfelder fehlen: {', '.join(missing)}")
    if not isinstance(msg["sensor_id"], str) or not msg["sensor_id"]:
        raise InvalidMessage("sensor_id muss ein nicht-leerer String sein")
    try:
        datetime.fromisoformat(str(msg["timestamp"]))
    except ValueError:
        raise InvalidMessage(f"ungültiger timestamp: {msg['timestamp']!r}")
    if msg["type"] not in accepted_types:
        raise InvalidMessage(f"Sensortyp {msg['type']!r} gehört nicht in diese Queue")
    value = msg["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidMessage(f"value ist keine gültige Zahl: {value!r}")
    low, high = VALID_RANGE[msg["type"]]
    if not low <= value <= high:
        raise InvalidMessage(f"value {value} außerhalb des plausiblen Bereichs [{low}, {high}]")
    return msg


def classify(sensor_type, value):
    warning, critical = config.THRESHOLDS[sensor_type]
    if value > critical:
        return "CRITICAL", critical
    if value > warning:
        return "WARNING", warning
    return None, None


class Analyzer:
    def __init__(self, name, queue, sensor_types, aggregate_types, args):
        self.name = name
        self.queue = queue
        self.sensor_types = sensor_types
        self.aggregate_types = aggregate_types
        self.args = args
        self.received = 0
        self.window = {t: [] for t in aggregate_types}
        self.window_start = time.time()

    # --- Verbindung -------------------------------------------------------------
    def make_callback(self, connection):
        # Eigener Channel mit Publisher Confirms für Alarme, eigener für Metriken
        self.alarm_channel = connection.channel()
        self.alarm_channel.confirm_delivery()
        self.metrics = Metrics(connection.channel(), self.name)
        self.connection = connection
        connection.call_later(config.WINDOW_SECONDS, self.flush_window)
        return self.on_message

    # --- Aggregation ---------------------------------------------------------------
    def flush_window(self):
        start, end = self.window_start, time.time()
        for sensor_type, values in self.window.items():
            unit = config.UNITS[sensor_type]
            if not values:
                log.info("[Fenster %.0fs] %s: keine gültigen Messwerte", end - start, sensor_type)
                continue
            result = {
                "sensor_type": sensor_type,
                "count": len(values),
                "avg": round(statistics.fmean(values), 2),
                "median": round(statistics.median(values), 2),
                "min": min(values),
                "max": max(values),
                "window_start": datetime.fromtimestamp(start, timezone.utc).isoformat(timespec="seconds"),
                "window_end": datetime.fromtimestamp(end, timezone.utc).isoformat(timespec="seconds"),
            }
            log.info(
                "[Fenster %.0fs] %s: n=%d  Ø=%.2f%s  Median=%.2f  Min=%.2f  Max=%.2f",
                end - start, sensor_type, result["count"], result["avg"], unit,
                result["median"], result["min"], result["max"],
            )
            self.metrics.emit("aggregate", **result)
        self.window = {t: [] for t in self.aggregate_types}
        self.window_start = end
        self.connection.call_later(config.WINDOW_SECONDS, self.flush_window)

    # --- Alarmierung -----------------------------------------------------------------
    def publish_alarm(self, msg, severity, threshold):
        alarm = {
            "alarm_id": str(uuid.uuid4()),
            "severity": severity,
            "sensor_id": msg["sensor_id"],
            "type": msg["type"],
            "value": msg["value"],
            "threshold": threshold,
            "unit": config.UNITS[msg["type"]],
            "measured_at": msg["timestamp"],
            "detected_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "source": self.name,
        }
        try:
            # Confirm-Modus: blockiert bis der Broker den Alarm bestätigt hat
            self.alarm_channel.basic_publish(
                config.EX_ALARMS, severity.lower(), json.dumps(alarm),
                properties=json_properties(priority=config.PRIORITY[severity]),
                mandatory=True,
            )
        except Exception as exc:
            raise ProcessingError(f"Alarm konnte nicht publiziert werden: {exc}")
        log.warning("ALARM %s: %s %s = %s%s (> %s)", severity, msg["type"], msg["sensor_id"],
                    msg["value"], alarm["unit"], threshold)
        self.metrics.emit("alarm", sensor_type=msg["type"], severity=severity)

    # --- Nachrichtenverarbeitung -------------------------------------------------------
    def on_message(self, channel, method, properties, body):
        started = time.perf_counter()
        self.received += 1
        headers = properties.headers or {}
        delivery_count = headers.get("x-delivery-count", 0)
        sensor_type = method.routing_key.split(".")[1] if method.routing_key.count(".") >= 2 else None
        try:
            msg = validate(body, self.sensor_types)

            if self.args.crash_after and self.received >= self.args.crash_after:
                log.error("SIMULIERTER ABSTURZ vor ACK (delivery_tag=%s, %s %s)",
                          method.delivery_tag, msg["sensor_id"], msg["value"])
                os._exit(1)
            if self.args.fail_on_poison and msg.get("poison"):
                raise ProcessingError("simulierter Verarbeitungsfehler (poison=true)")
            if self.args.slow:
                time.sleep(self.args.slow)

            severity, threshold = classify(msg["type"], msg["value"])
            if severity:
                self.publish_alarm(msg, severity, threshold)
            if msg["type"] in self.window:
                self.window[msg["type"]].append(float(msg["value"]))

            channel.basic_ack(method.delivery_tag)
            elapsed_ms = (time.perf_counter() - started) * 1000
            self.metrics.emit("processed", sensor_type=msg["type"], processing_ms=round(elapsed_ms, 3))
            log.info("OK  %-12s %-10s %8.2f%s%s", msg["type"], msg["sensor_id"], msg["value"],
                     config.UNITS[msg["type"]],
                     f"  (Zustellung #{delivery_count + 1}, redelivered)" if method.redelivered else "")

        except InvalidMessage as exc:
            # Permanenter Fehler: erneute Zustellung ist sinnlos -> direkt in die DLQ
            log.error("REJECT (-> DLQ) ungültige Nachricht: %s | body=%r", exc, body[:120])
            channel.basic_reject(method.delivery_tag, requeue=False)
            self.metrics.emit("failed", sensor_type=sensor_type, reason="invalid", detail=str(exc))

        except ProcessingError as exc:
            # Temporärer Fehler: Requeue; die Quorum Queue begrenzt die Versuche.
            # basic_reject (nicht basic_nack), damit x-delivery-count hochgezählt wird.
            attempt = delivery_count + 1
            final = attempt > config.DELIVERY_LIMIT
            log.error("REJECT requeue (Fehlversuch %d, Limit %d%s): %s", attempt, config.DELIVERY_LIMIT,
                      " überschritten -> DLQ" if final else "", exc)
            channel.basic_reject(method.delivery_tag, requeue=True)
            self.metrics.emit("failed", sensor_type=sensor_type, reason="processing_error",
                              attempt=attempt, detail=str(exc))


def main(name, queue, sensor_types, aggregate_types, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--prefetch", type=int, default=10)
    parser.add_argument("--crash-after", type=int, default=0,
                        help="Resilienztest: Prozess bei der N-ten Nachricht vor dem ACK beenden")
    parser.add_argument("--fail-on-poison", action="store_true",
                        help="Resilienztest: Nachrichten mit \"poison\": true lösen einen Fehler aus")
    parser.add_argument("--slow", type=float, default=0.0, help="künstliche Verarbeitungszeit in s")
    args = parser.parse_args()
    setup_logging(name)
    analyzer = Analyzer(name, queue, sensor_types, aggregate_types, args)
    run_consumer(name, queue, analyzer.make_callback, args.prefetch)
