# Pathling Auswertungen zu FHIR ImagingStudies

Dieses containerisierte Tool startet alle notwendigen Abhängigkeiten von Pathling und führt ein Analyseskript aus.

## Abhängigkeiten installieren

```
python3 pip install requirements.txt
```

## Ausführung

```
docker compose up
```

## Umgebungsvariablen

Die Anwendung kann über das `compose.yaml` konfiguriert werden.

* `KAFKA_BOOTSTRAP_SERVER`: KAFKA_BOOTSTRAP_SERVER (die Anwendung liest aus Apache Kafka)
* `KAFKA_IMAGING_STUDY_TOPIC: "fhir.pacs.imagingStudy"`: Kafka Topic, aus dem die Anwendung liest
* `PARTITION_A: "0"`
* `PARTITION_B: "1"`: aufgrund der Größe des Kafka Topics werden nur zwei Partitionen zeitgleich verarbeitet. Um das gesamte Topic zu verarbeiten, diese Parameter iterativ im Bereich 0 - 11 ändern und die Anwendung mehrmals starten.
