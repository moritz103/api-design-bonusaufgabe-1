"""Monitoring-Consumer: wertet Metrik-Events aus q.monitoring aus.

Alle Komponenten publizieren Events an den fanout-Exchange "monitoring".
Zusätzlich werden die Queue-Längen und Consumer-Zahlen per passivem
queue_declare direkt beim Broker abgefragt. Es gibt keinen Zugriff auf den
Speicher der Analyse-Consumer - alle Informationen kommen über RabbitMQ.
"""

import argparse
import json
import logging
from collections import Counter

from common import config
from common.rabbit import run_consumer, setup_logging

log = logging.getLogger("monitoring")

WATCHED_QUEUES = (config.Q_CLIMATE, config.Q_VIBRATION, config.Q_ALARMS, config.Q_DLQ)


class Monitor:
    def __init__(self, interval):
        self.interval = interval
        self.processed = Counter()          # je Sensortyp
        self.failed = Counter()             # je Grund
        self.alarms = Counter()             # je Severity (erzeugt)
        self.alarms_by_type = Counter()     # je Sensortyp
        self.alarms_handled = Counter()     # je Severity (vom Alarm-Consumer bearbeitet)
        self.dead_lettered = Counter()      # je Grund (aus x-death)
        self.publish_retries = 0
        self.proc_ms_sum = 0.0
        self.proc_ms_count = 0
        self.proc_ms_max = 0.0
        self.last_aggregate = {}

    def make_callback(self, connection):
        self.connection = connection
        self.stats_channel = connection.channel()
        connection.call_later(self.interval, self.report)
        return self.on_message

    def on_message(self, channel, method, properties, body):
        try:
            ev = json.loads(body)
            kind = ev["event"]
        except (ValueError, KeyError, TypeError):
            channel.basic_reject(method.delivery_tag, requeue=False)
            return
        if kind == "processed":
            self.processed[ev.get("sensor_type")] += 1
            ms = float(ev.get("processing_ms", 0))
            self.proc_ms_sum += ms
            self.proc_ms_count += 1
            self.proc_ms_max = max(self.proc_ms_max, ms)
        elif kind == "failed":
            self.failed[ev.get("reason", "unbekannt")] += 1
        elif kind == "alarm":
            self.alarms[ev.get("severity")] += 1
            self.alarms_by_type[ev.get("sensor_type")] += 1
        elif kind == "alarm_handled":
            self.alarms_handled[ev.get("severity")] += 1
        elif kind == "dead_lettered":
            self.dead_lettered[ev.get("reason", "unbekannt")] += 1
        elif kind == "publish_retry":
            self.publish_retries += int(ev.get("retries", 0))
        elif kind == "aggregate":
            self.last_aggregate[ev["sensor_type"]] = ev
        channel.basic_ack(method.delivery_tag)

    def queue_stats(self):
        rows = []
        for q in WATCHED_QUEUES:
            try:
                res = self.stats_channel.queue_declare(q, passive=True).method
                rows.append(f"{q}={res.message_count} msg/{res.consumer_count} cons")
            except Exception:
                self.stats_channel = self.connection.channel()
                rows.append(f"{q}=?")
        return "  ".join(rows)

    def report(self):
        total = sum(self.processed.values())
        avg = self.proc_ms_sum / self.proc_ms_count if self.proc_ms_count else 0.0
        fmt = lambda c: ", ".join(f"{k}={v}" for k, v in sorted(c.items(), key=lambda kv: str(kv[0]))) or "-"
        lines = [
            "=" * 72,
            f"Verarbeitete Nachrichten : {total}",
            f"  je Sensortyp           : {fmt(self.processed)}",
            f"Anomalien / Alarme       : {sum(self.alarms.values())} ({fmt(self.alarms)}) | je Typ: {fmt(self.alarms_by_type)}",
            f"  bearbeitet (Alarm-Cons): {fmt(self.alarms_handled)}",
            f"Fehlgeschlagen           : {sum(self.failed.values())} ({fmt(self.failed)})",
            f"In DLQ eingegangen       : {sum(self.dead_lettered.values())} ({fmt(self.dead_lettered)})",
            f"Producer-Wiederholungen  : {self.publish_retries}",
            f"Verarbeitungszeit        : Ø {avg:.2f} ms, max {self.proc_ms_max:.2f} ms",
        ]
        for t, a in sorted(self.last_aggregate.items()):
            lines.append(f"Letztes Fenster {t:<11}: n={a['count']} Ø={a['avg']} min={a['min']} max={a['max']}")
        lines.append(f"Queues: {self.queue_stats()}")
        lines.append("=" * 72)
        print("\n".join(lines), flush=True)
        self.connection.call_later(self.interval, self.report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=5.0, help="Ausgabeintervall in s")
    args = parser.parse_args()
    setup_logging("monitoring")
    run_consumer("monitoring-consumer", config.Q_MONITORING, Monitor(args.interval).make_callback, prefetch=50)


if __name__ == "__main__":
    main()
