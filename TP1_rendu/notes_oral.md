# Fiche d'oral : TP 1, de l'ingestion au KPI

Environ 1 minute par slide, soit 15 minutes. Les chiffres en gras sont à dire à voix haute.

## Slide par slide

1. **Titre.** « Le temps réel et le consolidé ne donnent pas le même chiffre d'affaires. J'ai monté la chaîne complète
   et j'ai chiffré l'écart à l'euro près. »
2. **Mission.** Présenter le fil A → F. Dire tout de suite que les données d'examen n'étaient pas disponibles :
   un jeu de test au **même format**, avec les **mêmes anomalies**, a été généré par un script à graine fixe ; il suffit
   de remplacer les CSV pour tout recalculer.
3. **A1.** 20 Mo/s entrants ; ×2 pour la réplication (**40 Mo/s**) ; 3 lectures complètes (**60 Mo/s**).
   **5,2 To** sur 3 jours, **15,6 To** répliqués. 12 partitions, parce qu'il en faut au moins 10 pour 10 consommateurs.
   4 brokers : 3 pour le stockage, plus 1 de marge.
4. **A2.** Raconter la panne : kafka3 arrêté, l'ISR tombe à 2, `acks=all` et `min.insync=3` provoquent un
   **refus** (`NOT_ENOUGH_REPLICAS`). Insister : le message est refusé, pas perdu. Avec `min.insync=2`, l'écriture
   passe. Règle : **min.insync = RF − 1**.
5. **B1/B2.** Même topic, trois watermarks, l'heure d'émission lue dans Kafka (`LogAppendTime`).
   5 s : **−2,2 %** en 5,6 s ; 60 s : **0 %** en 60,7 s. Fraude → 5 s ; rapport → au moins le retard maximal, ou Spark.
6. **B3/B4.** Le TaskManager s'arrête, le job passe en RESTARTING puis repart du dernier checkpoint. Ce qui a été écrit
   après ce checkpoint est réécrit : **338 doublons** au 2^e^ essai, 0 au 1^er^ (le checkpoint venait de passer).
   Avec l'exactly-once : **0 doublon**, mais les résultats arrivent au checkpoint (9,1 s au lieu de 5,5 s).
7. **C.** **26,2 millions** de fichiers, **7,9 Go** de mémoire NameNode. Un fichier par heure divise par 60.
   Démo : split en 21 fichiers = 42 objets ; après compaction, 1 fichier = 2 objets, même contenu (md5).
8. **D1/D2.** Un seul `agg` pour tous les contrôles. **377 doublons**, 190 pays absents, 249 quantités ≤ 0…
   Preuve : **11 449 + 658 = 12 107**.
9. **D3/D4.** P007 = **40 %** des lignes ; une partition 25,8 fois plus grosse que la médiane. Le broadcast supprime
   le shuffle des commandes : déséquilibre **1,0**, durée divisée par 2.
10. **E1.** **3,87 M€**, 11 449 commandes, **5,43 %** de rejet, la France pèse 38,6 %.
11. **E2.** Le moment clé. Les règles Flink, rejouées, redonnent exactement le fichier Flink : la méthode est donc
    validée. Écart **−156 166 €** = **+96 k** de doublons + **+55 k** de pays absent − **308 k** d'événements tardifs.
    « L'écart net de 4 % cache 12 % d'écarts bruts. »
12. **E2–E4.** Recommandations dans l'ordre de leur effet. KPI : Spark est la **source de vérité**, Flink reste un
    chiffre provisoire.
13. **F.** Suivre une commande sur le schéma : chemin chaud en haut, chemin froid en bas, contrôle d'écart entre les deux.
14. **Risques.** Chaque parade a été vue dans une partie du TP (A2, B4, C). Amélioration : un seul nettoyage, partagé.
15. **Conclusion.** Les 4 phrases, puis les questions.

## Questions probables

**Pourquoi pas `min.insync.replicas=1` ?** Kafka accepterait alors un message écrit sur une seule copie ; si ce broker
meurt avant la réplication, le message est perdu. Avec 2, on garde au moins 2 copies tout en tolérant 1 panne.

**Différence entre `acks=all` et `min.insync.replicas` ?** `acks` est réglé côté producteur (« j'attends l'accord de
toutes les répliques à jour »), `min.insync` côté topic (« il faut au moins N répliques à jour pour accepter »).
L'un sans l'autre ne garantit rien : `acks=all` avec un ISR réduit à 1 réplique revient à `acks=1`.

**Pourquoi le watermark 30 s perd-il encore 0,2 % ?** Le générateur retarde jusqu'à 40 s : les événements retardés
de plus de 30 s arrivent après la fermeture de la fenêtre.

**Pourquoi 0 doublon au premier essai de panne ?** Le checkpoint s'est terminé juste après l'émission : les alertes
étaient déjà couvertes par l'état sauvegardé, rien n'a été rejoué. Les doublons n'apparaissent que si la panne tombe
entre une émission et le checkpoint suivant ; c'est pour ça qu'il faut l'exactly-once et ne pas compter sur la chance.

**Pourquoi le broadcast et pas le salting ?** La table produits fait 40 lignes : la diffuser ne coûte rien et supprime
le shuffle. Le salting sert quand les deux tables sont grosses ; ici, il ajouterait du code sans rien gagner.

**Comment être sûr que la décomposition de l'écart est juste ?** Deux contrôles : les règles Flink rejouées redonnent
exactement `ca_minute_flink.csv` (0 € d'écart sur 120 minutes), et la somme des causes retombe exactement sur l'écart
(assertion dans le notebook).

**Les données sont-elles les vraies ?** Non : le kit d'examen n'était pas disponible. Le jeu de test reproduit le format
et les anomalies décrites dans l'énoncé. Le notebook recalcule tout si l'on remplace les trois CSV.

**Pourquoi Spark est-il la source de vérité ?** Il voit toutes les données, même très en retard, il dédoublonne,
il applique les contrôles qualité et on peut le relancer. Flink est rapide mais incomplet par construction : son
watermark écarte les retardataires.
