-- Exercice intégré, partie 2.2 : nombre d'événements par statut et par minute (fenêtre tumbling)
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/exp_init.sql -f /opt/sql/2_2_statut_par_minute.sql

SELECT window_start, window_end, statut, COUNT(*) AS nb_evenements
FROM TABLE(
  TUMBLE(TABLE expeditions, DESCRIPTOR(ts_evenement), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, statut;
