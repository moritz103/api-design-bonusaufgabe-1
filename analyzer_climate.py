"""Analyse-Consumer "Klima": Temperatur und Luftfeuchtigkeit.

Erhält über die Bindings sensor.temperature.* und sensor.humidity.* nur
Klimadaten (Filterung durch RabbitMQ-Routing). Aggregiert beide Typen über
10-s-Fenster und erkennt Anomalien (Temperatur > 80/100 °C, Feuchte > 85/95 %).
"""

from common import config
from common.analyzer import main

if __name__ == "__main__":
    main("analyzer-climate", config.Q_CLIMATE,
         sensor_types={"temperature", "humidity"},
         aggregate_types=("temperature", "humidity"),
         description=__doc__)
