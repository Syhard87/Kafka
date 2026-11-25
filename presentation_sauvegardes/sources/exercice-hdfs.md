# Exercice intégré : HDFS (parties 3 et 4)

HDFS 3.4.1, un NameNode et un DataNode.

    $ hdfs fsck /datalake/bronze/expeditions/ingestion_date=2026-11-23/expeditions_v1.jsonl -files -blocks -locations
    ... 1100094 bytes, replicated: replication=1, 1 block(s):  OK
     Number of data-nodes:  1
    $ hdfs getconf -confKey dfs.blocksize
    134217728
    $ hdfs getconf -confKey dfs.replication
    1

Fichier écrit par Spark sans régler la réplication :

    /datalake/silver/expeditions/part-00000-....snappy.parquet 73625 bytes, replicated: replication=3, 1 block(s):  Under replicated
    Target Replicas is 3 but found 1 live replica(s)

Couches : bronze (brut, tel que reçu), silver et gold recalculées par le notebook Spark à partir de bronze.
