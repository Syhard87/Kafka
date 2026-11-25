# Exercice intégré : panne du broker kafka2 (partie 1.3)

Topic `expeditions` : 6 partitions, réplication 3, `min.insync.replicas=2`.

    $ ./kafka-topics.sh --create --topic expeditions --partitions 6 --replication-factor 3 --config min.insync.replicas=2

Pendant le lot 4551, à 22:50:03 : `docker stop kafka2`. Leaders des partitions 0 et 4 passés de 2 à 3,
ISR réduit à 2 répliques sur chaque partition. Redémarrage à 22:50:40, kafka2 revenu dans chaque ISR.

Sortie du producteur :

    Lot 4551 : 100 expéditions, 325 événements vers 'expeditions' en 3.6 min environ, clé = id_expedition.
    %6|1795647006.064|FAIL| ... localhost:29092/2: Disconnected: connection closed by peer
    Terminé : 325 événements envoyés, 325 confirmés, 0 erreurs.

Topic de test `test_misr3` (réplication 3, `min.insync.replicas=3`), kafka2 arrêté : écriture refusée
(`NOT_ENOUGH_REPLICAS`) après 3 essais.

Réplication 3 : chaque partition existe en 3 copies, une sur chaque broker.
