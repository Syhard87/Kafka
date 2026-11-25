#!/usr/bin/env python3
"""Fabrique le fichier historique expeditions_v1.jsonl (exercice intégré, 3iL).

Historique des événements d'expéditions créées entre le 1er septembre et le 21 novembre 2026,
observé le 23 novembre 2026 à 00:00. Une ligne JSON par événement.

Le fichier contient volontairement des défauts, à traiter dans la couche silver :
  - des statuts invalides (fautes de frappe, statuts inconnus) ;
  - des horodatages manquants (null ou chaîne vide) ;
  - des doublons exacts (événements envoyés deux fois).

    python creer_historique.py                  # écrit expeditions_v1.jsonl (graine fixe : fichier reproductible)
"""
import argparse
import json
import random
from datetime import datetime, timedelta

OBSERVATION = datetime(2026, 11, 23, 0, 0, 0)
PAYS_UE = {"FR", "DE", "IT", "ES", "PL", "BE", "NL"}

FOURNISSEURS = [
    ("Aciéries du Rhin", "DE", ["NordFret", "TransEurope Express"], (3, 5)),
    ("Lumitech Milano", "IT", ["TransEurope Express", "MedCargo"], (4, 6)),
    ("Polska Komponenty", "PL", ["NordFret", "RapidRoute"], (4, 6)),
    ("Iberia Plast", "ES", ["MedCargo", "RapidRoute"], (3, 6)),
    ("Lyon Électronique", "FR", ["RapidRoute", "TransEurope Express"], (2, 3)),
    ("Atlas Textiles", "MA", ["MedCargo"], (8, 12)),
    ("Anatolia Metal", "TR", ["MedCargo", "TransEurope Express"], (8, 12)),
    ("Shenzhen Microparts", "CN", ["AsiaLink Logistics"], (25, 35)),
    ("Pune Precision", "IN", ["AsiaLink Logistics"], (25, 35)),
    ("Ohio Bearings", "US", ["NordFret", "AsiaLink Logistics"], (12, 18)),
]
# Probabilité de livrer dans le délai promis, propre à chaque transporteur
PONCTUALITE = {"NordFret": 0.93, "TransEurope Express": 0.86, "RapidRoute": 0.80,
               "MedCargo": 0.70, "AsiaLink Logistics": 0.62}
TAUX_PERDUES = 0.04          # expéditions bloquées en route, jamais livrées
STATUTS_INVALIDES = ["LIVRÉ", "livre", "ANNULE", "INCONNU", ""]


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nb", type=int, default=1200)
    p.add_argument("--seed", type=int, default=2026)
    p.add_argument("--sortie", default="expeditions_v1.jsonl")
    a = p.parse_args()
    rng = random.Random(a.seed)

    debut = datetime(2026, 9, 1)
    fin = datetime(2026, 11, 21, 23, 59)
    evenements = []
    for i in range(a.nb):
        fournisseur, pays, transporteurs, (dmin, dmax) = rng.choice(FOURNISSEURS)
        transporteur = rng.choice(transporteurs)
        delai = rng.randint(dmin, dmax)
        creation = debut + timedelta(seconds=rng.uniform(0, (fin - debut).total_seconds()))
        a_l_heure = rng.random() < PONCTUALITE[transporteur]
        duree_j = delai * (rng.uniform(0.55, 1.0) if a_l_heure else rng.uniform(1.05, 1.7))
        statuts = ["CREE", "EN_TRANSIT"] + ([] if pays in PAYS_UE else ["EN_DOUANE"]) + ["LIVRE"]
        if rng.random() < TAUX_PERDUES:
            statuts = statuts[:rng.randint(1, len(statuts) - 1)]
        # instants : départ entre 0,1 et 0,3 de la durée, douane vers 0,6, livraison à la fin
        fractions = {"CREE": 0.0, "EN_TRANSIT": rng.uniform(0.1, 0.3), "EN_DOUANE": rng.uniform(0.5, 0.7),
                     "LIVRE": 1.0}
        base = {"id_expedition": f"EXP-H-{i + 1:05d}", "transporteur": transporteur,
                "fournisseur": fournisseur, "pays_origine": pays, "pays_dest": "FR",
                "delai_promis_j": delai, "poids_kg": round(rng.uniform(20, 2500), 1)}
        contact = "logistique@" + fournisseur.lower().replace(" ", "-").replace("é", "e").replace("É", "e") + ".example"
        for s in statuts:
            ts = creation + timedelta(days=duree_j * fractions[s])
            if ts >= OBSERVATION:
                break  # pas encore arrivé à la date d'observation
            e = {"id_expedition": base["id_expedition"], "statut": s, "ts_evenement": fmt(ts)}
            e.update({k: base[k] for k in ("transporteur", "fournisseur", "pays_origine", "pays_dest",
                                           "delai_promis_j", "poids_kg")})
            e["contact_email"] = contact if s == "CREE" else None
            evenements.append((ts, e))

    evenements.sort(key=lambda x: x[0])
    lignes = [e for _, e in evenements]

    # Défauts volontaires, sur des événements distincts
    idx = list(range(len(lignes)))
    rng.shuffle(idx)
    n_statut = round(len(lignes) * 0.012)
    n_ts = round(len(lignes) * 0.008)
    n_doublons = round(len(lignes) * 0.02)
    for j in idx[:n_statut]:
        lignes[j]["statut"] = rng.choice(STATUTS_INVALIDES)
    for k, j in enumerate(idx[n_statut:n_statut + n_ts]):
        lignes[j]["ts_evenement"] = None if k % 2 == 0 else ""
    a_doubler = set(idx[n_statut + n_ts:n_statut + n_ts + n_doublons])

    with open(a.sortie, "w", encoding="utf-8") as f:
        for j, e in enumerate(lignes):
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
            if j in a_doubler:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"{a.sortie} : {len(lignes) + n_doublons} lignes ({len(lignes)} événements distincts, "
          f"{n_statut} statuts invalides, {n_ts} horodatages manquants, {n_doublons} doublons).")


if __name__ == "__main__":
    main()
