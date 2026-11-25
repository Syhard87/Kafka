-- Exercice intégré, partie 2.1 : table source « expeditions » sur le topic Kafka
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/exp_init.sql
--
-- ts_evenement est le temps d'événement ; le watermark tolère 10 s de désordre.
-- Lecture depuis le début du topic (earliest-offset) : toutes les expéditions déjà publiées.
-- Checkpoint toutes les 10 s pour les jobs lancés depuis cette session (reprise après panne, 2.4).

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE IF NOT EXISTS expeditions (
  id_expedition  STRING,
  statut         STRING,
  ts_evenement   TIMESTAMP(3),
  transporteur   STRING,
  fournisseur    STRING,
  pays_origine   STRING,
  pays_dest      STRING,
  delai_promis_j INT,
  poids_kg       DOUBLE,
  contact_email  STRING,
  WATERMARK FOR ts_evenement AS ts_evenement - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'expeditions',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'properties.group.id' = 'flink-expeditions',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
