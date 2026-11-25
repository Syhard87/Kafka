# Documentation Apache (mécanismes de sauvegarde)

## Kafka

- Rétention par défaut : `log.retention.hours = 168`, soit 7 jours.
  https://kafka.apache.org/documentation/#brokerconfigs_log.retention.hours
- MirrorMaker 2 : copie continue des topics d'un cluster vers un autre (géo-réplication).
  https://kafka.apache.org/documentation/#georeplication

## Flink

- Checkpoints incrémentaux : avec RocksDB, un checkpoint n'envoie que les fichiers créés depuis le
  précédent ; les fichiers communs sont partagés entre checkpoints. Avec HashMap, chaque checkpoint
  contient tout l'état.
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/large_state_tuning/#incremental-checkpoints
- Savepoints : déclenchés à la main, complets, conservés, pour arrêter, mettre à jour ou migrer un job.
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints_vs_savepoints/

## HDFS

- Réplication par défaut : `dfs.replication = 3`, soit 3 copies de chaque bloc.
  https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
- Snapshots : copie en lecture seule d'un dossier, créée instantanément ; aucun bloc n'est copié,
  seules les modifications ultérieures sont enregistrées.
  https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html
- DistCp : copie vers un autre cluster ; `-update` ne copie que les fichiers modifiés ; `-diff` copie
  la différence entre deux snapshots.
  https://hadoop.apache.org/docs/r3.4.1/hadoop-distcp/DistCp.html
