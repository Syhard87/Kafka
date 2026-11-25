-- Exercice intégré, partie 2.4 : même détection, avec un sink Kafka exactly-once
-- Les résultats sont écrits dans des transactions validées à chaque checkpoint (10 s) :
-- après une panne, un consommateur en read_committed ne voit aucun doublon.
-- Prérequis : topic expeditions_bloquees_eo créé.
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/exp_init.sql -f /opt/sql/2_4_bloquees_exactly_once.sql
-- Lecture : kafka-console-consumer.sh --topic expeditions_bloquees_eo --isolation-level read_committed ...

CREATE TABLE IF NOT EXISTS expeditions_bloquees_eo (
  id_expedition  STRING,
  debut_session  TIMESTAMP(3),
  fin_session    TIMESTAMP(3),
  nb_evenements  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'expeditions_bloquees_eo',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'key.format' = 'raw',
  'key.fields' = 'id_expedition',
  'value.format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'exp-bloquees'
);

INSERT INTO expeditions_bloquees_eo
SELECT id_expedition,
       SESSION_START(ts_evenement, INTERVAL '90' SECOND) AS debut_session,
       SESSION_END(ts_evenement, INTERVAL '90' SECOND)   AS fin_session,
       COUNT(*) AS nb_evenements
FROM expeditions
GROUP BY id_expedition, SESSION(ts_evenement, INTERVAL '90' SECOND)
HAVING SUM(CASE WHEN statut = 'LIVRE' THEN 1 ELSE 0 END) = 0;
