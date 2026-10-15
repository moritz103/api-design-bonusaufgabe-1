# Bonusaufgabe 1 – Echtzeit-Analyse von IoT-Sensordaten mit RabbitMQ

Python 3 + Pika, getestet mit RabbitMQ 4.3.6. Ausführliche Beschreibung: `../intro.html`,
Resilienztests: `scenarios.md`.

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
source .venv/bin/activate
python -m common.topology          # optional – jede Komponente deklariert die Topologie selbst
```

RabbitMQ muss auf `localhost:5672` laufen (`guest/guest`), änderbar über
`RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASSWORD`.

## Start (je ein Terminal)

```bash
python monitoring_consumer.py
python dlq_consumer.py
python alarm_consumer.py
python analyzer_climate.py
python analyzer_vibration.py
python producer.py --type all --interval 0.5 --anomaly-rate 0.1
```

Einzelne Sensoren: `python producer.py --type temperature --sensor-id temp-02`.

## Dateien

| Datei | Inhalt |
|---|---|
| `common/config.py` | Namen, TTL, Delivery-Limit, Schwellwerte, Backoff-Parameter |
| `common/topology.py` | Exchanges, Queues, Bindings (idempotent) |
| `common/rabbit.py` | Verbindung mit Exponential Backoff, `ReliablePublisher`, Consumer-Schleife |
| `common/analyzer.py` | Validierung, Anomalieerkennung, Alarm-Publish, 10-s-Fenster, ACK/REJECT |
| `common/metrics.py` | Metrik-Events an den Monitoring-Exchange |
| `producer.py` | Sensor-Simulation (temperature, humidity, vibration) |
| `analyzer_climate.py` / `analyzer_vibration.py` | Analyse-Consumer |
| `alarm_consumer.py` | Alarmverarbeitung nach Priorität |
| `monitoring_consumer.py` | Kennzahlen alle 5 s |
| `dlq_consumer.py` | Ausgabe der Dead-Letter-Gründe |
| `tools/inject.py` | Gezielte Fehlernachrichten für die Resilienztests |

## Resilienztests (Kurzfassung)

```bash
python -m tools.inject bad-value                  # ungültig        -> DLQ (rejected)
python analyzer_climate.py --fail-on-poison       # dann:
python -m tools.inject poison                     # 4 Fehlversuche  -> DLQ (delivery_limit)
python analyzer_climate.py --crash-after 5        # Absturz vor ACK -> Redelivery nach Neustart
# Analyzer stoppen, Producer laufen lassen, 60 s warten -> DLQ (expired)
python -m tools.inject alarm-burst                # bei gestopptem Alarm-Consumer: CRITICAL zuerst
rabbitmqctl list_connections pid client_properties | grep producer   # dann:
rabbitmqctl close_connection "<pid>" "Netzwerktest"                  # Producer verbindet neu
RABBIT_PORT=5999 python producer.py               # Backoff 1/2/4/8/10 s, dann Abbruch
```
