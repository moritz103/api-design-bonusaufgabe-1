"""Metrik-Events an den Monitoring-Exchange (fanout) senden.

Monitoring ist bewusst "fire and forget": transiente Nachrichten ohne
Publisher Confirms, Fehler werden nur geloggt. Ein Ausfall des Monitorings
darf die eigentliche Verarbeitung nie blockieren.
"""

import json
import logging
import time

from . import config
from .rabbit import json_properties

log = logging.getLogger("metrics")


class Metrics:
    def __init__(self, channel, component):
        self.channel = channel
        self.component = component

    def emit(self, event, **fields):
        payload = {"component": self.component, "event": event, "ts": time.time(), **fields}
        try:
            self.channel.basic_publish(
                config.EX_MONITORING, "", json.dumps(payload),
                properties=json_properties(persistent=False),
            )
        except Exception as exc:
            log.warning("Metrik '%s' konnte nicht gesendet werden: %s", event, exc)
