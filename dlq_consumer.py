"""DLQ-Consumer: zeigt, warum Nachrichten in der Dead Letter Queue gelandet sind.

Liest die Header x-first-death-reason / x-first-death-queue bzw. x-death
(rejected, expired, delivery_limit) und meldet jeden Eingang an das Monitoring.
"""

import argparse
import logging

from common import config
from common.metrics import Metrics
from common.rabbit import run_consumer, setup_logging

log = logging.getLogger("dlq")


def make_callback(connection):
    metrics = Metrics(connection.channel(), "dlq-consumer")

    def on_message(channel, method, properties, body):
        headers = properties.headers or {}
        deaths = headers.get("x-death") or [{}]
        reason = headers.get("x-first-death-reason") or deaths[0].get("reason", "unbekannt")
        queue = headers.get("x-first-death-queue") or deaths[0].get("queue", "?")
        reason = reason.decode() if isinstance(reason, bytes) else reason
        queue = queue.decode() if isinstance(queue, bytes) else queue
        log.warning("DLQ  grund=%-14s queue=%-22s key=%-28s count=%s  body=%s",
                    reason, queue, method.routing_key, deaths[0].get("count", 1),
                    body.decode(errors="replace")[:160])
        metrics.emit("dead_lettered", reason=reason, queue=queue)
        channel.basic_ack(method.delivery_tag)

    return on_message


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    setup_logging("dlq-consumer")
    run_consumer("dlq-consumer", config.Q_DLQ, make_callback, prefetch=10)


if __name__ == "__main__":
    main()
