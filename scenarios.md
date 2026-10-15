# Resilienztests

Alle Szenarien sind gezielt per Kommandozeile auslösbar und wurden mit
RabbitMQ 4.3.6 durchgespielt. Die Ausgaben stammen aus diesen Testläufen.

## 1. Consumer-Ausfall während der Verarbeitung

`python analyzer_climate.py --crash-after 5` → danach normal neu starten.

1. **Auslöser:** Der Analyzer beendet sich bei der 5. Nachricht per `os._exit(1)` – nach der Validierung, aber *vor* dem ACK.
2. **Nachricht:** Sie ist an den Consumer ausgeliefert, aber unbestätigt. Beim Schließen der TCP-Verbindung legt der Broker alle unbestätigten Nachrichten (bis zu `prefetch=10`) zurück in die Queue.
3. **ACK/NACK:** Keins von beiden – der Broker erkennt den Verbindungsabbruch.
4. **Erneute Zustellung:** Ja. Nach dem Neustart: `OK temperature temp-01 60.40°C (Zustellung #2, redelivered)`. `x-delivery-count` steigt dabei auf 1 (seit RabbitMQ 4.3 zählt ein Verbindungsabbruch als fehlgeschlagene Zustellung).
5. **Fehlerbehandlung:** Manuelles ACK erst nach vollständiger Verarbeitung → At-least-once. Keine Nachricht geht verloren; Duplikate sind möglich (siehe Fazit).
6. **DLQ:** Bringt dieselbe Nachricht den Consumer mehr als 3-mal zum Absturz, überschreitet sie das `x-delivery-limit` → DLQ (`delivery_limit`). Bleibt der Consumer länger als 60 s weg → `expired`.
7. **Weiterarbeit:** Ja, der neu gestartete Analyzer arbeitet den Rückstau ab.

## 2. Syntaktisch / semantisch ungültige Nachricht

`python -m tools.inject bad-value` (auch `invalid-json`, `missing-field`).

1. **Auslöser:** `value: "heiß"`, abgeschnittenes JSON oder fehlender `timestamp`.
2. **Nachricht:** `validate()` wirft `InvalidMessage` (permanenter Fehler).
3. **ACK/NACK:** `basic_reject(requeue=False)`.
4. **Erneute Zustellung:** Nein – eine erneute Zustellung würde wieder fehlschlagen.
5. **Fehlerbehandlung:** Unterscheidung permanent/temporär; Metrik-Event `failed/invalid` ans Monitoring.
6. **DLQ:** Sofort über `x-dead-letter-exchange=dlx`: `DLQ grund=rejected queue=q.analysis.climate`.
7. **Weiterarbeit:** Ja, die folgende Nachricht wird normal verarbeitet.

## 3. Verarbeitungsfehler (Poison Message)

`python analyzer_climate.py --fail-on-poison` + `python -m tools.inject poison`.

1. **Auslöser:** Gültige Nachricht mit `"poison": true` → `ProcessingError` (simuliert z. B. eine nicht erreichbare Abhängigkeit).
2. **Nachricht:** Wird als temporärer Fehler behandelt und zurück in die Queue gelegt.
3. **ACK/NACK:** `basic_reject(requeue=True)`.
4. **Erneute Zustellung:** Ja, `x-delivery-count` steigt 1 → 2 → 3.
5. **Fehlerbehandlung:** Die Quorum Queue begrenzt die Versuche (`x-delivery-limit=3`) – im Code ist kein eigener Zähler nötig.
6. **DLQ:** Beim 4. Fehlversuch (delivery-count > 3): `DLQ grund=delivery_limit`.
7. **Weiterarbeit:** Ja; andere Nachrichten werden zwischendurch normal verarbeitet.

**Erkenntnis aus dem Test:** Mit `basic_nack(requeue=True)` lief die Nachricht in einer Endlosschleife (> 30 000 Zustellungen in wenigen Sekunden). Seit RabbitMQ 4.3 erhöht nur `basic.reject` den delivery-count („Verarbeitung fehlgeschlagen“); `basic.nack` bedeutet „nicht verarbeitet“ und wird nicht gezählt.

## 4. TTL-Ablauf

Analyzer stoppen, Producer laufen lassen, > 60 s warten.

1. **Auslöser:** Kein Consumer an `q.analysis.vibration`.
2. **Nachricht:** Bleibt in der Queue, bis `x-message-ttl=60000` abläuft. Die Queue pendelt sich bei ~60 s Daten ein (im Test ~193 Nachrichten bei 0,3-s-Intervall).
3. **ACK/NACK:** Keins – der Broker entfernt die Nachricht selbst.
4. **Erneute Zustellung:** Nein.
5. **Fehlerbehandlung:** Veraltete Sensordaten werden nicht mehr analysiert; der Rückstau ist begrenzt.
6. **DLQ:** Abgelaufene Nachrichten gehen über den DLX: `DLQ grund=expired queue=q.analysis.vibration`.
7. **Weiterarbeit:** Ja, nach dem Neustart werden nur noch aktuelle Daten verarbeitet.

## 5. Netzwerkunterbrechung des Producers

`rabbitmqctl close_connection "<pid>" "Netzwerktest"`; vollständiger Backoff: `RABBIT_PORT=5999 python producer.py`.

1. **Auslöser:** Der Broker trennt die Verbindung bzw. der Broker ist nicht erreichbar.
2. **Nachricht:** Kein Publisher Confirm → gilt als nicht zugestellt und wird erneut gesendet.
3. **ACK/NACK:** Publisher Confirms (Broker → Producer); ohne ACK wird erneut gesendet.
4. **Erneute Zustellung:** Ja: `Publish fehlgeschlagen (ConnectionClosedByBroker) - Wiederholung 1/5 in 1 s`, danach erfolgreich.
5. **Fehlerbehandlung:** Exponential Backoff 1 → 2 → 4 → 8 → 10 s (gedeckelt), max. 5 Wiederholungen, danach Abbruch mit Exit-Code 1.
6. **DLQ:** Nicht betroffen – die Nachricht hat den Broker nie erreicht.
7. **Weiterarbeit:** Ja, nach erfolgreichem Reconnect läuft die Simulation weiter.
