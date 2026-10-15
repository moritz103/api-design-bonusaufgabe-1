"""Analyse-Consumer "Maschine": Vibration.

Erhält über das Binding sensor.vibration.* ausschließlich Vibrationsdaten.
Aggregiert über 10-s-Fenster und erkennt Anomalien (> 7 / 11 mm/s).
"""

from common import config
from common.analyzer import main

if __name__ == "__main__":
    main("analyzer-vibration", config.Q_VIBRATION,
         sensor_types={"vibration"},
         aggregate_types=("vibration",),
         description=__doc__)
