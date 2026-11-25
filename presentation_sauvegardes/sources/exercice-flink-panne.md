# Exercice intégré : panne du TaskManager Flink (partie 2.4)

Mesures relevées pendant l'exécution réelle de l'exercice (Flink 1.20.1, Kafka 3 brokers), le 25/11/2026.

## Réglage des checkpoints (flink/exp_init.sql)

    SET 'execution.checkpointing.interval' = '10s';

## Compteurs de checkpoints du job at-least-once, avant la panne (API REST de Flink)

    checkpoints : {'restored': 0, 'total': 6, 'in_progress': 0, 'completed': 6, 'failed': 0}
      6 COMPLETED 22:57:24
      5 COMPLETED 22:57:14

## Déroulé de la panne

    Dernière expédition bloquée reçue avant la panne :
    23:00:09.780 p0 {"id_expedition":"EXP-7620-0027", ... ,"nb_evenements":2}
    dernier checkpoint réussi : n° 22 à 23:00:04.546
    23:00:10.039 $ docker compose stop taskmanager
    23:00:15 état du job : RESTARTING
    23:00:30 $ docker compose start taskmanager
    23:00:47 état du job : RUNNING ['RUNNING', 'RUNNING']
    restauré depuis le checkpoint n° 22 : file:/flink-data/checkpoints/2d14586f54aefbaac6dc5676a5a96514/chk-22

Première alerte réécrite après la reprise : 23:00:45.027 (topic at-least-once).
Interruption : de 23:00:10 (arrêt) à 23:00:45 (premières écritures après reprise), soit 35 s.

## Résultats comparés

    at-least-once (expeditions_bloquees) : 42 lignes, 40 expéditions distinctes, 2 doublon(s)
    exactly-once, lecteur read_committed : 40 lignes, 40 expéditions distinctes, 0 doublon(s)
    exactly-once, lecteur read_uncommitted : 42 lignes, 40 expéditions distinctes, 2 doublon(s)

40 expéditions bloquées attendues (4 lots × 10, vérité terrain du générateur) : aucune perdue.
