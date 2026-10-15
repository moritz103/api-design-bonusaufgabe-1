"""Zentrale Konfiguration: Verbindung, Namen der Exchanges/Queues und Schwellwerte."""

import os

# --- Verbindung -------------------------------------------------------------
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")

# --- Exchanges ----------------------------------------------------------------
EX_SENSOR = "sensor.data"      # topic:  sensor.<typ>.<sensor_id>
EX_ALARMS = "alarms"           # direct: warning | critical
EX_MONITORING = "monitoring"   # fanout: Metrik-Events aller Komponenten
EX_DLX = "dlx"                 # fanout: Dead-Letter-Exchange

# --- Queues -------------------------------------------------------------------
Q_CLIMATE = "q.analysis.climate"
Q_VIBRATION = "q.analysis.vibration"
Q_ALARMS = "q.alarms"
Q_MONITORING = "q.monitoring"
Q_DLQ = "q.dlq"

# --- Queue-Parameter ----------------------------------------------------------
MESSAGE_TTL_MS = 60_000   # max. 60 s in den Hauptqueues
DELIVERY_LIMIT = 3        # max. Zustellversuche, danach Dead-Lettering
ALARM_MAX_PRIORITY = 10

# --- Sensortypen ----------------------------------------------------------------
SENSOR_TYPES = ("temperature", "humidity", "vibration")
UNITS = {"temperature": "°C", "humidity": "%", "vibration": "mm/s"}

# --- Alarme ---------------------------------------------------------------------
PRIORITY = {"WARNING": 5, "CRITICAL": 9}

# Schwellwerte je Sensortyp: (WARNING ab, CRITICAL ab)
THRESHOLDS = {
    "temperature": (80.0, 100.0),
    "humidity": (85.0, 95.0),
    "vibration": (7.0, 11.0),
}

# --- Aggregation ------------------------------------------------------------------
WINDOW_SECONDS = 10

# --- Producer-Retry -----------------------------------------------------------------
MAX_RETRIES = 5
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 10.0
