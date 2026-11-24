#!/usr/bin/env python3
"""TP1 partie C (et TP5 partie B) : consommateur Python du topic commandes.

Commit manuel APRÈS traitement (garantie at-least-once).
Le compteur par client est sauvegardé dans un fichier d'état, pour qu'une relance
reparte de là où le traitement s'était arrêté.

    python consumer.py                    # traitement normal, affichage toutes les 100 commandes
    python consumer.py --panne 500        # plante après le traitement du 500e message, avant son commit
    python consumer.py --idempotent       # ignore les order_id déjà traités (pas de double comptage)
    python consumer.py --lent 0.01        # consommateur volontairement lent (TP5 partie B)
    python consumer.py --reset-etat       # repart d'un compteur vide

Options utiles : --group (défaut tp1-py), --arret-inactif 10 (s'arrête après 10 s sans message).
"""
import argparse
import json
import os
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"


def parse_args():
    p = argparse.ArgumentParser(description="Consommateur du topic commandes (TP1)")
    p.add_argument("--group", default="tp1-py", help="groupe de consommateurs (défaut : tp1-py)")
    p.add_argument("--topic", default="commandes")
    p.add_argument("--panne", type=int, default=0,
                   help="lève une exception après N messages traités, avant le commit du N-ième")
    p.add_argument("--idempotent", action="store_true",
                   help="mémorise les order_id traités et ignore les doublons")
    p.add_argument("--lent", type=float, default=0.0, help="pause en secondes après chaque message")
    p.add_argument("--etat", default="etat_consumer.json", help="fichier d'état (compteurs, order_id vus)")
    p.add_argument("--reset-etat", action="store_true", help="supprime le fichier d'état au démarrage")
    p.add_argument("--arret-inactif", type=float, default=0,
                   help="s'arrête après N secondes sans message (0 = jamais)")
    return p.parse_args()


def charger_etat(fichier):
    if os.path.exists(fichier):
        with open(fichier, encoding="utf-8") as f:
            d = json.load(f)
        return Counter(d["nb"]), set(d["deja_traites"]), d["total"]
    return Counter(), set(), 0


def sauver_etat(fichier, nb, deja, total):
    tmp = fichier + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"nb": nb, "deja_traites": sorted(deja), "total": total}, f)
    os.replace(tmp, fichier)  # remplacement atomique : jamais de fichier à moitié écrit


def afficher(nb, total, doublons):
    top = ", ".join(f"{client}={n}" for client, n in nb.most_common(10))
    print(f"[{total} commandes comptées, {len(nb)} clients, {doublons} doublons ignorés] top 10 : {top}")


def main():
    args = parse_args()
    if args.reset_etat and os.path.exists(args.etat):
        os.remove(args.etat)
    nb, deja, total = charger_etat(args.etat)

    c = Consumer({"bootstrap.servers": BOOTSTRAP,
                  "group.id": args.group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([args.topic])

    lus = doublons = 0  # pour CE lancement
    premier = dernier_msg = None
    dernier_recu = time.monotonic()
    try:
        while True:
            m = c.poll(1.0)
            if m is None:
                if args.arret_inactif and time.monotonic() - dernier_recu > args.arret_inactif:
                    break
                continue
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    print("Erreur :", m.error())
                continue
            dernier_recu = time.monotonic()
            try:
                evt = json.loads(m.value())
            except ValueError:
                c.commit(message=m, asynchronous=False)  # message illisible : ignoré
                continue

            position = f"partition {m.partition()}, offset {m.offset()}, order_id {evt.get('order_id')}"
            if premier is None:
                premier = position
                print(f"Premier message lu : {position}")

            # --- traitement ---
            if args.idempotent and evt.get("order_id") in deja:
                doublons += 1
                print(f"Doublon ignoré : {position}")
            else:
                nb[evt["customer_id"]] += 1
                total += 1
                if args.idempotent:
                    deja.add(evt.get("order_id"))
                sauver_etat(args.etat, nb, deja, total)
            lus += 1
            dernier_msg = position
            if args.lent:
                time.sleep(args.lent)
            if lus % 100 == 0:
                afficher(nb, total, doublons)

            # --- panne simulée : traitement fait, commit pas encore fait ---
            if args.panne and lus == args.panne:
                raise RuntimeError(f"Panne simulée après le traitement du message {lus} ({position}), "
                                   f"avant son commit")

            # --- commit après traitement ---
            c.commit(message=m, asynchronous=False)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        c.close()
        print(f"Fin : {lus} messages lus dans ce lancement (dernier : {dernier_msg}).")
        afficher(nb, total, doublons)


if __name__ == "__main__":
    main()
