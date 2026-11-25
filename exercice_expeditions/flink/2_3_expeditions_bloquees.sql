-- Exercice intégré, partie 2.3 : expéditions bloquées
-- Une expédition est bloquée si son activité s'arrête plus de 90 secondes sans avoir reçu LIVRE.
-- Fenêtre de session par id_expedition (gap 90 s) : une session se ferme 90 s après le dernier
-- événement de l'expédition. On ne garde que les sessions qui ne contiennent aucun LIVRE.
-- Prérequis : topic expeditions_bloquees créé (3 partitions, réplication 3).
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/exp_init.sql -f /opt/sql/2_3_expeditions_bloquees.sql

CREATE TABLE IF NOT EXISTS expeditions_bloquees (
  id_expedition  STRING,
  debut_session  TIMESTAMP(3),
  fin_session    TIMESTAMP(3),
  nb_evenements  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'expeditions_bloquees',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'key.format' = 'raw',
  'key.fields' = 'id_expedition',
  'value.format' = 'json'
);

INSERT INTO expeditions_bloquees
SELECT id_expedition,
       SESSION_START(ts_evenement, INTERVAL '90' SECOND) AS debut_session,
       SESSION_END(ts_evenement, INTERVAL '90' SECOND)   AS fin_session,
       COUNT(*) AS nb_evenements
FROM expeditions
GROUP BY id_expedition, SESSION(ts_evenement, INTERVAL '90' SECOND)
HAVING SUM(CASE WHEN statut = 'LIVRE' THEN 1 ELSE 0 END) = 0;
