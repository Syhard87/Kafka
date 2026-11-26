#!/usr/bin/env python3
"""Génère un jeu de données de TEST au format du kit examen (Kit_Examen_Etudiants.zip).

Les vrais fichiers du kit n'étaient pas disponibles : ce script produit des fichiers
au même format, avec les mêmes types d'anomalies, pour pouvoir faire tout le TP.
Si vous récupérez les vrais fichiers, remplacez simplement les trois CSV : le notebook
Spark recalcule tout.

Fichiers produits (dans le dossier du script) :
  commandes_exam1.csv  : id_commande, id_entrepot, id_produit, pays, quantite,
                         prix_unitaire, ts_evenement, ts_ingestion
  produits_exam1.csv   : id_produit, nom_produit, famille, prix_catalogue
  ca_minute_flink.csv  : w_start, w_end, nb, total  (sortie simulée du job Flink du TP3)

Anomalies injectées (taux approximatifs) :
  - doublons sur id_commande (renvoi du producteur, ts_ingestion plus tardif) : ~3 %
  - pays absent : ~1,5 %          - quantité <= 0 : ~2 %
  - prix absent : ~1,2 %          - date d'événement invalide ou vide : ~0,7 %
  - événements tardifs (ts_ingestion très après ts_evenement) : ~10 %
  - skew : le produit P007 représente ~40 % des commandes

Le fichier ca_minute_flink.csv applique exactement les règles du job Flink décrites
dans l'énoncé (E2) : quantite > 0, prix non NULL, pas de dédoublonnage, pas de contrôle
du pays, événement ignoré si ts_ingestion > fin de sa minute + 5 s.

Usage : python generer_donnees_exam1.py   (graine fixe : résultat identique à chaque fois)
"""
import csv
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta

SEED = 2026
DEBUT = datetime(2026, 11, 2, 8, 0, 0)
NB_MINUTES = 120
NB_ENTREPOTS = 100
PRODUIT_CHAUD = "P007"
PART_PRODUIT_CHAUD = 0.40

# famille -> [(nom du produit, prix catalogue en euros HT)]
FAMILLES = {
    "Informatique": [("Ordinateur portable", 899.00), ("Écran 27 pouces", 289.00), ("Clavier", 39.90),
                     ("Souris", 24.90), ("Station d'accueil", 179.00), ("Disque SSD", 119.00),
                     ("Webcam", 69.90), ("Casque audio", 89.00)],
    "Réseau": [("Routeur", 249.00), ("Switch 24 ports", 329.00), ("Borne Wi-Fi", 159.00),
               ("Câble RJ45", 7.90), ("Pare-feu", 690.00)],
    "Mobilier": [("Bureau", 349.00), ("Chaise ergonomique", 279.00), ("Armoire", 420.00),
                 ("Caisson", 149.00), ("Lampe de bureau", 45.00)],
    "Outillage": [("Perceuse", 129.00), ("Visseuse", 99.00), ("Scie circulaire", 189.00),
                  ("Meuleuse", 85.00), ("Coffret d'outils", 75.00), ("Niveau laser", 139.00),
                  ("Escabeau", 89.00)],
    "Consommables": [("Ramette papier", 5.90), ("Cartouche d'encre", 34.90), ("Toner", 79.00),
                     ("Étiquettes", 14.90), ("Carton d'emballage", 2.50), ("Ruban adhésif", 3.20),
                     ("Gants de protection", 6.50), ("Piles", 9.90), ("Film étirable", 18.00),
                     ("Marqueurs", 7.50), ("Agrafes", 2.90), ("Enveloppes", 8.90), ("Bloc-notes", 3.90),
                     ("Classeurs", 4.50), ("Stylos", 6.90)],
}
PAYS = ["FR", "BE", "CH", "DE", "ES", "IT", "MA", "TN"]
POIDS_PAYS = [40, 12, 8, 14, 10, 9, 4, 3]

DATES_INVALIDES = ["2026-11-02 25:13:07.000", "2026-02-30 08:12:00.000", "02/11/2026 08:15",
                   "N/A", "", "2026-11-32 09:01:44.250", "0000-00-00 00:00:00.000"]


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def main():
    rng = random.Random(SEED)
    dossier = os.path.dirname(os.path.abspath(__file__))

    # ------------------------------------------------------------------ produits
    produits = []
    n = 1
    for famille, noms in FAMILLES.items():
        for nom, prix in noms:
            produits.append({"id_produit": f"P{n:03d}", "nom_produit": nom, "famille": famille,
                             "prix_catalogue": prix})
            n += 1
    prix_catalogue = {p["id_produit"]: p["prix_catalogue"] for p in produits}
    ids_produits = [p["id_produit"] for p in produits]
    autres_produits = [p for p in ids_produits if p != PRODUIT_CHAUD]

    # ----------------------------------------------------------------- commandes
    lignes = []
    id_cmd = 1
    for m in range(NB_MINUTES):
        debut_minute = DEBUT + timedelta(minutes=m)
        nb = max(60, int(rng.gauss(100, 12)))
        for _ in range(nb):
            ts_evt = debut_minute + timedelta(milliseconds=rng.randint(0, 59_999))
            if rng.random() < 0.10:                         # événement tardif
                delai = rng.uniform(5, 120)
            else:                                            # arrivée normale
                delai = min(rng.expovariate(1 / 0.8), 4.5)
            ts_ing = ts_evt + timedelta(seconds=delai)

            id_produit = PRODUIT_CHAUD if rng.random() < PART_PRODUIT_CHAUD else rng.choice(autres_produits)
            prix = round(prix_catalogue[id_produit] * rng.uniform(0.9, 1.1), 2)
            quantite = rng.choices([1, 2, 3, 4, 5, 6, 8, 10], [30, 22, 15, 10, 8, 6, 5, 4])[0]

            ligne = {
                "id_commande": f"CMD{id_cmd:07d}",
                "id_entrepot": f"E{rng.randint(1, NB_ENTREPOTS):03d}",
                "id_produit": id_produit,
                "pays": rng.choices(PAYS, POIDS_PAYS)[0],
                "quantite": quantite,
                "prix_unitaire": f"{prix:.2f}",
                "ts_evenement": fmt(ts_evt),
                "ts_ingestion": fmt(ts_ing),
                "_ts_ing": ts_ing,
            }
            # anomalies (indépendantes, plusieurs possibles sur une même ligne)
            if rng.random() < 0.015:
                ligne["pays"] = ""
            if rng.random() < 0.02:
                ligne["quantite"] = rng.choice([0, -1, -2, -5])
            if rng.random() < 0.012:
                ligne["prix_unitaire"] = ""
            if rng.random() < 0.007:
                ligne["ts_evenement"] = rng.choice(DATES_INVALIDES)
            lignes.append(ligne)
            id_cmd += 1

            # doublons : le producteur renvoie le message (retry) quelques secondes plus tard
            r = rng.random()
            nb_copies = 2 if r < 0.003 else (1 if r < 0.03 else 0)
            for _ in range(nb_copies):
                copie = dict(ligne)
                copie["_ts_ing"] = ligne["_ts_ing"] + timedelta(seconds=rng.uniform(1, 20))
                copie["ts_ingestion"] = fmt(copie["_ts_ing"])
                lignes.append(copie)

    # le fichier est un export du topic : ordre d'arrivée (ts_ingestion)
    lignes.sort(key=lambda l: l["_ts_ing"])
    colonnes = ["id_commande", "id_entrepot", "id_produit", "pays", "quantite", "prix_unitaire",
                "ts_evenement", "ts_ingestion"]
    with open(os.path.join(dossier, "commandes_exam1.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=colonnes, extrasaction="ignore")
        w.writeheader()
        w.writerows(lignes)

    with open(os.path.join(dossier, "produits_exam1.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id_produit", "nom_produit", "famille", "prix_catalogue"])
        w.writeheader()
        w.writerows(produits)

    # --------------------------------------------- simulation du job Flink (TP3)
    ca = defaultdict(lambda: [0, 0.0])
    for l in lignes:
        if int(l["quantite"]) <= 0 or l["prix_unitaire"] == "":
            continue
        try:
            ts_evt = datetime.strptime(l["ts_evenement"], "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            continue                      # message illisible : ignoré par Flink
        w_start = ts_evt.replace(second=0, microsecond=0)
        w_end = w_start + timedelta(minutes=1)
        if l["_ts_ing"] > w_end + timedelta(seconds=5):
            continue                      # arrivé après le watermark : ignoré
        ca[w_start][0] += 1
        ca[w_start][1] += int(l["quantite"]) * float(l["prix_unitaire"])

    with open(os.path.join(dossier, "ca_minute_flink.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["w_start", "w_end", "nb", "total"])
        for w_start in sorted(ca):
            nb, total = ca[w_start]
            w.writerow([w_start.strftime("%Y-%m-%d %H:%M:%S"),
                        (w_start + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        nb, f"{total:.2f}"])

    print(f"commandes_exam1.csv : {len(lignes)} lignes ; produits_exam1.csv : {len(produits)} produits ; "
          f"ca_minute_flink.csv : {len(ca)} minutes")


if __name__ == "__main__":
    main()
