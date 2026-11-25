# Sauvegarder une chaîne Lambda — slide-by-slide specification

Expanded from the outline in `sauvegardes-chaine-lambda.foundation.md`, page by page. Nine fields per slide.
Gate 2 (detailed-spec sign-off) was skipped: the owner asked for the deck to be built straight away.

## Slide 1 — La restauration juge la sauvegarde

- **Archetype.** A-10 (deviation: the outline said A-11; the slide carries a timeline because RPO and RTO are two spans on one time axis).
- **Title.** La restauration juge la sauvegarde
- **Bottom line.** Deux mesures comptent : les données qu'on accepte de perdre (RPO) et le temps pour repartir (RTO).
- **Structure.** Eyebrow, title, standfirst, one figure across the body, bottom line.
- **Text.** Eyebrow « Mesurer une sauvegarde : RPO et RTO » ; standfirst « Une copie ne compte qu'au moment où il faut repartir. »
- **Visuals.** Frise : dernière sauvegarde, panne, reprise ; accolade RPO (données perdues) et accolade RTO (service arrêté).
- **Animations.** Rise only (slide arrival).
- **Interactive elements.** None.
- **Sources.** none

## Slide 2 — Incrémentale légère, différentielle vite restaurée

- **Archetype.** A-05
- **Title.** Incrémentale légère, différentielle vite restaurée
- **Bottom line.** Après une panne le jeudi, l'incrémentale demande 4 copies à recharger, la différentielle seulement 2.
- **Structure.** Grille : 3 lignes (totale chaque jour, incrémentale, différentielle) × 4 jours, puis la colonne « Jeudi : panne ». Tier two : ce que copie chaque type.
- **Text.** Standfirst « Exemple construit : 4 fichiers, une totale le dimanche, puis un fichier modifié chaque jour. » ; sous chaque ligne : 12, 3 et 6 fichiers en 3 jours ; colonne de reprise : 1 copie, 4 copies, 2 copies.
- **Visuals.** Fichiers A à D en pastilles pleines, une colonne par jour.
- **Animations.** Les colonnes apparaissent un jour après l'autre (1 s d'écart), puis la colonne de reprise : encode le temps qui passe et ce qui est copié chaque jour. Licence `illustration`.
- **Interactive elements.** Panneau `scope` « Ce que copie chaque type ».
- **Sources.** modele-semaine

## Slide 3 — Deux briques gardent les données brutes

- **Archetype.** A-10
- **Title.** Deux briques gardent les données brutes
- **Bottom line.** Kafka et la zone bronze de HDFS gardent les données d'origine ; tout le reste se recalcule à partir d'elles.
- **Structure.** Deux lignes de boîtes : temps réel (générateur, Kafka, Flink, topic des alertes) et batch (historique, HDFS bronze, Spark, silver et gold). Tier two : tableau logiciel par sauvegarde.
- **Text.** Standfirst « En couleur : les données d'origine, que rien ne permet de recalculer. » ; sous chaque boîte, son mécanisme.
- **Visuals.** Kafka et HDFS bronze en couleur d'accent ; flèches libellées.
- **Animations.** Current sur les flèches : encode le sens du flux.
- **Interactive elements.** Panneau `scope` « Chaque logiciel et ses sauvegardes ».
- **Sources.** doc-apache

## Slide 4 — Kafka réplique, il ne sauvegarde pas

- **Archetype.** A-04
- **Title.** Kafka réplique, il ne sauvegarde pas
- **Bottom line.** La réplication survit à la panne d'un broker, mais une suppression par erreur est répliquée partout.
- **Structure.** Deux colonnes argumentées (couvre / ne couvre pas), une ligne de pied sur la rétention.
- **Text.** Couvre : panne d'un broker (testé : 325 messages confirmés sur 325, 0 erreur), perte d'un disque. Ne couvre pas : topic supprimé par erreur, perte de tout le site (MirrorMaker 2).
- **Visuals.** None (ledger).
- **Animations.** Rise only.
- **Interactive elements.** None.
- **Sources.** exercice-kafka-panne; doc-apache

## Slide 5 — Flink se sauvegarde lui-même, en continu

- **Archetype.** A-08
- **Title.** Flink se sauvegarde lui-même, en continu (deviation: the outline's title had 7 words, DS-091 caps at 6).
- **Bottom line.** Un checkpoint enregistre l'état et les offsets Kafka : complet avec HashMap, incrémental avec RocksDB.
- **Structure.** Deux lignes (HashMap, RocksDB) × trois checkpoints (n° 20, 21, 22), ligne de reprise. Tier two : contenu d'un checkpoint.
- **Text.** Standfirst « Dans l'exercice, un checkpoint toutes les 10 s. Le savepoint, lui, est une copie complète lancée à la main. »
- **Visuals.** Blocs d'état 1 à 6 : plein = écrit, pointillé = réutilisé. Schéma de principe.
- **Animations.** Les checkpoints apparaissent l'un après l'autre : encode leur succession dans le temps. Licence `illustration`.
- **Interactive elements.** Panneau `scope` « Ce que contient un checkpoint ».
- **Sources.** exercice-flink-panne; doc-apache

## Slide 6 — 35 s d'arrêt, zéro donnée perdue

- **Archetype.** A-09
- **Title.** 35 s d'arrêt, zéro donnée perdue (deviation: the outline's « Panne réelle : 35 s, zéro perte » counted 7 words).
- **Bottom line.** Le job est reparti du checkpoint n° 22 : aucune donnée perdue, 35 s d'interruption.
- **Structure.** Frise de 23:00:00 à 23:00:50, cinq événements, deux accolades. Tier two : doublons at-least-once ou exactly-once.
- **Text.** Checkpoint n° 22 (23:00:04.5), TaskManager arrêté (23:00:10), RESTARTING (23:00:15), TaskManager relancé (23:00:30), reprise depuis le n° 22 (23:00:45) ; « Interruption : 35 s (RTO) » ; « Relu depuis Kafka : 0 perte ».
- **Visuals.** Frise horizontale ; le checkpoint n° 22 est le point de décision.
- **Animations.** Les événements apparaissent dans l'ordre chronologique. Licence `illustration`.
- **Interactive elements.** Panneau `condition` « Sans exactly-once : des doublons ».
- **Sources.** exercice-flink-panne

## Slide 7 — Bronze sauvegardé, silver et gold recalculés

- **Archetype.** A-10
- **Title.** Bronze sauvegardé, silver et gold recalculés
- **Bottom line.** HDFS protège bronze par réplication et snapshots ; silver et gold se reconstruisent en relançant Spark.
- **Structure.** Bronze → silver → gold ; accolade « aucune copie » ; trois lignes de mécanismes sous bronze. Tier two : le piège de la réplication Spark.
- **Text.** Réplication (3 copies en production, 1 dans l'exercice), snapshot, DistCp.
- **Visuals.** Boîtes et flèches libellées, bronze en accent.
- **Animations.** Current sur les flèches.
- **Interactive elements.** Panneau `condition` « Le piège rencontré avec Spark ».
- **Sources.** exercice-hdfs; doc-apache

## Slide 8 — Protégez d'abord ce qui est irremplaçable

- **Archetype.** A-14
- **Title.** Protégez d'abord ce qui est irremplaçable
- **Bottom line.** Avant de choisir totale, incrémentale ou différentielle, vérifiez si la donnée peut être reconstruite à partir d'une autre.
- **Structure.** Close : la phrase centrée, la ligne du bas centrée.
- **Text.** Eyebrow « Avant de choisir une sauvegarde ».
- **Visuals.** None.
- **Animations.** Turn on the headline.
- **Interactive elements.** None.
- **Sources.** none

## Open — needs a decision

| # | The question | Why it matters | Proposed |
| :-- | :--- | :--- | :--- |
| 1 | Garder `lang="fr"` alors que le squelette du shell impose `lang="en"` | `shell.py check` signale l'écart ; une page française déclarée en anglais est mal lue par les lecteurs d'écran | Garder `fr` |
