-- TP6 partie 1 : pipeline complet de détection d'alertes
-- Lancement (checkpoints toutes les 10 s, réglés dans init.sql) :
--   docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/pipeline.sql
-- Prérequis : topics commandes_enrichies (6 partitions), ca_pays_minute (3) et alertes (3) créés.
--
-- Les trois INSERT sont regroupés dans un STATEMENT SET : un seul job Flink, une seule lecture
-- du topic commandes, un seul état et un seul checkpoint pour les trois sorties.
--
-- Watermark : celui de la table commandes (ts - 5 s, dans init.sql). Le générateur produit des
-- événements quasi ordonnés ; 5 s couvrent le désordre entre partitions et le délai réseau, et
-- les résultats par minute sortent 5 s après la fin de la fenêtre. Un délai plus long donnerait
-- des comptes plus complets en cas de retard, mais des alertes plus tardives (voir TP4 partie B).

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

-- Chiffre d'affaires (en euros) et nombre de commandes par pays et par minute.
-- Clé = pays : les résultats d'un même pays restent ordonnés.
CREATE TABLE IF NOT EXISTS ca_pays_minute (
  country  STRING,
  w_start  TIMESTAMP(3),
  w_end    TIMESTAMP(3),
  nb       BIGINT,
  ca_eur   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ca_pays_minute',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'key.format' = 'raw',
  'key.fields' = 'country',
  'value.format' = 'json'
);

-- Alertes : plus de 3 commandes en une minute, ou plus de 1 000 euros cumulés en une minute.
-- Améliorations par rapport à 05_alertes.sql :
--   - montants en euros (amount_eur), cohérents avec commandes_enrichies ;
--   - motif précis, qui distingue les trois cas (nombre, montant, les deux) ;
--   - niveau de gravité, pour trier les alertes côté consommateur ;
--   - clé = customer_id : les alertes d'un client sont ordonnées et faciles à dédoublonner.
CREATE TABLE IF NOT EXISTS alertes (
  customer_id STRING,
  w_start     TIMESTAMP(3),
  w_end       TIMESTAMP(3),
  nb          BIGINT,
  total       DOUBLE,
  motif       STRING,
  gravite     STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'alertes',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'key.format' = 'raw',
  'key.fields' = 'customer_id',
  'value.format' = 'json'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO commandes_enrichies
SELECT order_id, customer_id, product, amount,
       ROUND(amount * 0.92, 2) AS amount_eur,
       UPPER(country) AS country,
       ts
FROM commandes
WHERE amount > 0;

INSERT INTO ca_pays_minute
SELECT UPPER(country) AS country, window_start, window_end,
       COUNT(*) AS nb,
       ROUND(SUM(amount * 0.92), 2) AS ca_eur
FROM TABLE(
  TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY UPPER(country), window_start, window_end;

INSERT INTO alertes
SELECT customer_id, window_start, window_end,
       COUNT(*) AS nb,
       ROUND(SUM(amount * 0.92), 2) AS total,
       CASE
         WHEN COUNT(*) > 3 AND SUM(amount * 0.92) > 1000 THEN 'plus de 3 commandes et plus de 1000 euros en 1 minute'
         WHEN COUNT(*) > 3 THEN 'plus de 3 commandes en 1 minute'
         ELSE 'plus de 1000 euros en 1 minute'
       END AS motif,
       CASE
         WHEN COUNT(*) > 3 AND SUM(amount * 0.92) > 1000 THEN 'HAUTE'
         ELSE 'MOYENNE'
       END AS gravite
FROM TABLE(
  TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY customer_id, window_start, window_end
HAVING COUNT(*) > 3 OR SUM(amount * 0.92) > 1000;

END;
