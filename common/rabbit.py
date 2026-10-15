"""Verbindungsaufbau mit Exponential Backoff, zuverlässiges Publizieren und Consumer-Schleife."""

import json
import logging
import time

import pika
from pika.exceptions import AMQPChannelError, AMQPConnectionError, UnroutableError

from . import config
from .topology import declare_topology

log = logging.getLogger("rabbit")


def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{name}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Pika-interne Meldungen nur bei Fehlern anzeigen
    logging.getLogger("pika").setLevel(logging.CRITICAL)


def backoff_delay(retry):
    """Wartezeit vor dem retry-ten Wiederholungsversuch: 1, 2, 4, 8, 10 s (gedeckelt)."""
    return min(config.BACKOFF_BASE_S * 2 ** (retry - 1), config.BACKOFF_MAX_S)


def connection_params(name):
    return pika.ConnectionParameters(
        host=config.RABBIT_HOST,
        port=config.RABBIT_PORT,
        credentials=pika.PlainCredentials(config.RABBIT_USER, config.RABBIT_PASSWORD),
        heartbeat=30,
        blocked_connection_timeout=30,
        connection_attempts=1,  # Wiederholungen steuern wir selbst (Backoff)
        client_properties={"connection_name": name},
    )


def connect_with_backoff(name):
    """Baut eine Verbindung auf; bei Fehlern max. MAX_RETRIES Wiederholungen mit Backoff."""
    for retry in range(config.MAX_RETRIES + 1):
        try:
            conn = pika.BlockingConnection(connection_params(name))
            if retry:
                log.info("Verbindung nach %d Wiederholung(en) wiederhergestellt", retry)
            return conn
        except AMQPConnectionError as exc:
            if retry == config.MAX_RETRIES:
                log.error("Verbindung endgültig fehlgeschlagen nach %d Wiederholungen", retry)
                raise
            delay = backoff_delay(retry + 1)
            log.warning(
                "Verbindung fehlgeschlagen (%s) - Wiederholung %d/%d in %.0f s",
                type(exc).__name__, retry + 1, config.MAX_RETRIES, delay,
            )
            time.sleep(delay)


def json_properties(priority=None, persistent=True, headers=None):
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2 if persistent else 1,
        priority=priority,
        headers=headers,
    )


class ReliablePublisher:
    """Publisher mit Publisher Confirms, mandatory-Flag und Reconnect per Exponential Backoff."""

    def __init__(self, name):
        self.name = name
        self.connection = None
        self.channel = None

    def _connect(self):
        self.connection = pika.BlockingConnection(connection_params(self.name))
        self.channel = self.connection.channel()
        declare_topology(self.channel)
        self.channel.confirm_delivery()

    def _reset(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
        self.connection = self.channel = None

    def publish(self, exchange, routing_key, payload, priority=None):
        """Publiziert persistent und wartet auf das Broker-ACK.

        Gibt die Anzahl benötigter Wiederholungen zurück. Wirft nach
        MAX_RETRIES Fehlversuchen die letzte Exception weiter.
        """
        body = json.dumps(payload) if not isinstance(payload, (bytes, str)) else payload
        for retry in range(config.MAX_RETRIES + 1):
            try:
                if self.channel is None:
                    self._connect()
                self.channel.basic_publish(
                    exchange, routing_key, body,
                    properties=json_properties(priority=priority),
                    mandatory=True,
                )
                return retry
            except UnroutableError:
                # Kein Netzwerkfehler: keine Queue passt zum Routing Key -> nicht wiederholen
                log.error("Nachricht nicht routbar (exchange=%s, key=%s)", exchange, routing_key)
                raise
            except (AMQPConnectionError, AMQPChannelError) as exc:
                self._reset()
                if retry == config.MAX_RETRIES:
                    log.error("Publish endgültig fehlgeschlagen nach %d Wiederholungen", retry)
                    raise
                delay = backoff_delay(retry + 1)
                log.warning(
                    "Publish fehlgeschlagen (%s) - Wiederholung %d/%d in %.0f s",
                    type(exc).__name__, retry + 1, config.MAX_RETRIES, delay,
                )
                time.sleep(delay)

    def close(self):
        self._reset()


def run_consumer(name, queue, make_callback, prefetch):
    """Consumer-Schleife mit manuellen ACKs und automatischem Reconnect.

    make_callback(connection) wird nach jedem (Re-)Connect aufgerufen und
    liefert den on_message-Callback (so können Publisher-Channels und Timer
    an die neue Verbindung gebunden werden).
    """
    while True:
        connection = connect_with_backoff(name)
        try:
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=prefetch)
            channel.basic_consume(queue, make_callback(connection), auto_ack=False)
            log.info("Warte auf Nachrichten in '%s' (prefetch=%d)", queue, prefetch)
            channel.start_consuming()
        except KeyboardInterrupt:
            log.info("Beendet.")
            try:
                connection.close()
            except Exception:
                pass
            return
        except AMQPConnectionError as exc:
            log.warning("Verbindung verloren (%s) - baue neu auf", type(exc).__name__)
