-- TP3 partie C : commandes valides enrichies, de Kafka (commandes) vers Kafka (commandes_enrichies)
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/03_enrichissement.sql
-- Prérequis : topic commandes_enrichies créé (6 partitions, réplication 3).
-- Clé des messages de sortie = customer_id : toutes les commandes d'un client restent dans
-- la même partition, donc dans l'ordre, comme dans le topic source.

CREATE TABLE IF NOT EXISTS commandes_enrichies (
  order_id    BIGINT,
  customer_id STRING,
  product     STRING,
  amount      DOUBLE,
  amount_eur  DOUBLE,
  country     STRING,
  ts          TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'commandes_enrichies',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'key.format' = 'raw',
  'key.fields' = 'customer_id',
  'value.format' = 'json'
);

-- amount_eur : taux fixe de 0,92 par hypothèse.
-- WHERE amount > 0 : filtrage au plus tôt, les montants invalides ne vont pas plus loin.
-- (Pas de commentaire en fin de ligne : le client SQL de Flink les gère mal en mode -f.)
INSERT INTO commandes_enrichies
SELECT order_id,
       customer_id,
       product,
       amount,
       ROUND(amount * 0.92, 2) AS amount_eur,
       UPPER(country) AS country,
       ts
FROM commandes
WHERE amount > 0;
