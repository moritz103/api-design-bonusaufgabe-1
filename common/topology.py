"""Deklariert die komplette RabbitMQ-Topologie (idempotent).

Jede Komponente ruft declare_topology() beim Verbindungsaufbau auf. Dadurch
ist die Startreihenfolge egal. Standalone: python -m common.topology
"""

from . import config


def declare_topology(channel):
    # Exchanges: drei verschiedene Typen (topic, direct, fanout)
    channel.exchange_declare(config.EX_SENSOR, exchange_type="topic", durable=True)
    channel.exchange_declare(config.EX_ALARMS, exchange_type="direct", durable=True)
    channel.exchange_declare(config.EX_MONITORING, exchange_type="fanout", durable=True)
    channel.exchange_declare(config.EX_DLX, exchange_type="fanout", durable=True)

    # Dead Letter Queue: sammelt abgelehnte, abgelaufene und zu oft zugestellte Nachrichten
    channel.queue_declare(config.Q_DLQ, durable=True)
    channel.queue_bind(config.Q_DLQ, config.EX_DLX)

    # Hauptqueues der Analyse: Quorum Queues mit TTL, DLX und Zustell-Limit
    analysis_args = {
        "x-queue-type": "quorum",
        "x-message-ttl": config.MESSAGE_TTL_MS,
        "x-dead-letter-exchange": config.EX_DLX,
        "x-delivery-limit": config.DELIVERY_LIMIT,
    }
    channel.queue_declare(config.Q_CLIMATE, durable=True, arguments=analysis_args)
    channel.queue_bind(config.Q_CLIMATE, config.EX_SENSOR, routing_key="sensor.temperature.*")
    channel.queue_bind(config.Q_CLIMATE, config.EX_SENSOR, routing_key="sensor.humidity.*")

    channel.queue_declare(config.Q_VIBRATION, durable=True, arguments=analysis_args)
    channel.queue_bind(config.Q_VIBRATION, config.EX_SENSOR, routing_key="sensor.vibration.*")

    # Alarm-Queue: Priority Queue (classic), keine TTL - Alarme dürfen nicht verfallen
    channel.queue_declare(
        config.Q_ALARMS,
        durable=True,
        arguments={
            "x-max-priority": config.ALARM_MAX_PRIORITY,
            "x-dead-letter-exchange": config.EX_DLX,
        },
    )
    for severity in config.PRIORITY:
        channel.queue_bind(config.Q_ALARMS, config.EX_ALARMS, routing_key=severity.lower())

    # Monitoring-Queue: empfängt alle Metrik-Events
    channel.queue_declare(config.Q_MONITORING, durable=True)
    channel.queue_bind(config.Q_MONITORING, config.EX_MONITORING)


if __name__ == "__main__":
    from .rabbit import connect_with_backoff

    conn = connect_with_backoff("topology-setup")
    declare_topology(conn.channel())
    conn.close()
    print("Topologie deklariert.")
