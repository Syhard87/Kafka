# Sauvegarder une chaîne Lambda — slide-by-slide specification

Expanded from the outline in `sauvegardes-chaine-lambda.foundation.md`, page by page. Nine fields per slide.
Gate 2 (detailed-spec sign-off) was skipped: the owner asked for the deck to be built straight away.
Revision of 26/11/2026: the RPO/RTO slide was removed and the deck refocused on the backup types (slides 1, 3 and 4 added, slides 5, 7 and 8 reworded in terms of types).

## Slide 1 — Tout dépend du point de comparaison

- **Archetype.** A-10
- **Title.** Tout dépend du point de comparaison
- **Bottom line.** La totale copie tout ; l'incrémentale, les changements depuis la veille ; la différentielle, depuis la dernière totale.
- **Structure.** Trois lignes (totale, incrémentale, différentielle) × quatre jours, chaque sauvegarde reliée par un arc à ce qu'elle compare.
- **Text.** Standfirst « Chaque sauvegarde copie ce qui a changé depuis un point de départ, et ce point n'est pas le même. » ; libellés « compare à la veille », « compare à la totale ».
- **Visuals.** Boîtes pleines pour les totales, boîtes vides pour incr. et diff. ; arcs fléchés vers le point de comparaison.
- **Animations.** Rise only.
- **Interactive elements.** None.
- **Sources.** none

## Slide 2 — Incrémentale légère, différentielle vite restaurée

- **Archetype.** A-05
- **Title.** Incrémentale légère, différentielle vite restaurée
- **Bottom line.** Après une panne le jeudi, l'incrémentale demande 4 copies à recharger, la différentielle seulement 2.
- **Structure.** Grille : 3 lignes × 4 jours, puis la colonne « Jeudi : panne ». Tier two : ce que copie chaque type.
- **Text.** Standfirst « Exemple construit : 4 fichiers, une totale le dimanche, puis un fichier modifié chaque jour. » ; 12, 3 et 6 fichiers en 3 jours ; 1, 4 et 2 copies à recharger.
- **Visuals.** Fichiers A à D en pastilles, une colonne par jour.
- **Animations.** Les colonnes apparaissent un jour après l'autre (1 s d'écart) : encode le temps qui passe. Licence `illustration`.
- **Interactive elements.** Panneau `scope` « Ce que copie chaque type ».
- **Sources.** modele-semaine

## Slide 3 — Chaque type gagne sur un critère

- **Archetype.** A-04
- **Title.** Chaque type gagne sur un critère
- **Bottom line.** En pratique, on combine : une totale chaque semaine, puis une incrémentale ou une différentielle chaque jour.
- **Structure.** Tableau : critères (copie, durée, place, restauration, point faible) × trois types ; le gagnant de chaque critère en accent. Tier two : pourquoi une totale régulière.
- **Text.** Voir le tableau du deck.
- **Visuals.** None (tableau).
- **Animations.** Rise only.
- **Interactive elements.** Panneau `condition` « Pourquoi une totale régulière ».
- **Sources.** modele-semaine

## Slide 4 — Chaque brique a ses propres sauvegardes

- **Archetype.** A-06
- **Title.** Chaque brique a ses propres sauvegardes
- **Bottom line.** Kafka et HDFS répliquent en continu, Flink et HDFS font de la totale et de l'incrémentale, Spark recalcule.
- **Structure.** Matrice : Kafka, Flink, HDFS, Spark × copie continue, totale, incrémentale, différentielle.
- **Text.** Standfirst « La différentielle n'y apparaît pas : aucune des quatre briques ne la propose d'elle-même. »
- **Visuals.** None (matrice).
- **Animations.** Rise only.
- **Interactive elements.** None.
- **Sources.** doc-apache

## Slide 5 — Kafka réplique, il ne sauvegarde pas

- **Archetype.** A-04
- **Title.** Kafka réplique, il ne sauvegarde pas
- **Bottom line.** La réplication survit à la panne d'un broker, mais une suppression par erreur est répliquée partout.
- **Structure.** Deux colonnes argumentées (couvre / ne couvre pas), une ligne de pied sur la rétention.
- **Text.** Couvre : panne d'un broker (testé : 325 messages confirmés sur 325, 0 erreur), perte d'un disque. Ne couvre pas : topic supprimé par erreur, perte de tout le site (MirrorMaker 2, copie incrémentale en continu).
- **Visuals.** None (ledger).
- **Animations.** Rise only.
- **Interactive elements.** None.
- **Sources.** exercice-kafka-panne; doc-apache

## Slide 6 — Flink se sauvegarde lui-même, en continu

- **Archetype.** A-08
- **Title.** Flink se sauvegarde lui-même, en continu
- **Bottom line.** Un checkpoint enregistre l'état et les offsets Kafka : complet avec HashMap, incrémental avec RocksDB.
- **Structure.** Deux lignes (HashMap, RocksDB) × trois checkpoints, ligne de reprise. Tier two : contenu d'un checkpoint.
- **Text.** Standfirst « Dans l'exercice, un checkpoint toutes les 10 s. Le savepoint, lui, est une copie complète lancée à la main. »
- **Visuals.** Blocs d'état : plein = écrit, pointillé = réutilisé. Schéma de principe.
- **Animations.** Les checkpoints apparaissent l'un après l'autre. Licence `illustration`.
- **Interactive elements.** Panneau `scope` « Ce que contient un checkpoint ».
- **Sources.** exercice-flink-panne; doc-apache

## Slide 7 — 35 s d'arrêt, zéro donnée perdue

- **Archetype.** A-09
- **Title.** 35 s d'arrêt, zéro donnée perdue
- **Bottom line.** Le job est reparti du checkpoint n° 22 : aucune donnée perdue, 35 s d'interruption.
- **Structure.** Frise de 23:00:00 à 23:00:50, cinq événements, deux accolades. Tier two : doublons.
- **Text.** Checkpoint n° 22 (23:00:04.5), TaskManager arrêté (23:00:10), RESTARTING (23:00:15), TaskManager relancé (23:00:30), reprise depuis le n° 22 (23:00:45) ; « Interruption : 35 s » ; « Relu depuis Kafka : 0 perte ».
- **Visuals.** Frise horizontale ; le checkpoint n° 22 est le point de décision.
- **Animations.** Les événements apparaissent dans l'ordre chronologique. Licence `illustration`.
- **Interactive elements.** Panneau `condition` « Sans exactly-once : des doublons ».
- **Sources.** exercice-flink-panne

## Slide 8 — Bronze sauvegardé, silver et gold recalculés

- **Archetype.** A-10
- **Title.** Bronze sauvegardé, silver et gold recalculés
- **Bottom line.** HDFS protège bronze par réplication et snapshots ; silver et gold se reconstruisent en relançant Spark.
- **Structure.** Bronze → silver → gold ; accolade « aucune copie » ; trois lignes de mécanismes avec leur type. Tier two : le piège de la réplication Spark.
- **Text.** Réplication (copie continue : 3 copies en production, 1 dans l'exercice), snapshot (incrémentale), DistCp (totale puis incrémentale avec -update).
- **Visuals.** Boîtes et flèches libellées, bronze en accent.
- **Animations.** Current sur les flèches.
- **Interactive elements.** Panneau `condition` « Le piège rencontré avec Spark ».
- **Sources.** exercice-hdfs; doc-apache

## Slide 9 — Sauvegardez la source, recalculez le reste

- **Archetype.** A-14
- **Title.** Sauvegardez la source, recalculez le reste
- **Bottom line.** Une totale puis des incrémentales, réservées aux données que rien ne recalcule : Kafka et bronze.
- **Structure.** Close : la phrase centrée, la ligne du bas centrée.
- **Text.** Eyebrow « Ce qu'il faut retenir ».
- **Visuals.** None.
- **Animations.** Turn on the headline.
- **Interactive elements.** None.
- **Sources.** none

## Open — needs a decision

| # | The question | Why it matters | Proposed |
| :-- | :--- | :--- | :--- |
| 1 | Garder `lang="fr"` alors que le squelette du shell impose `lang="en"` | `shell.py check` signale l'écart ; une page française déclarée en anglais est mal lue par les lecteurs d'écran | Garder `fr` |
