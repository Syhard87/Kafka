#!/usr/bin/env python3
"""TP3 Q7 : compare ca_minute (calculé par Flink) avec un calcul direct sur le topic commandes.

Relit les deux topics depuis le début, regroupe les commandes valides (amount > 0) par minute
de leur champ ts (temps de l'événement), puis affiche les deux résultats côte à côte.

    python verif_ca_minute.py
"""
import json
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"


def lire_tout(topic):
    """Lit toutes les partitions d'un topic, du début jusqu'à l'offset de fin actuel."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verif-ca-minute",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    meta = c.list_topics(topic, timeout=10)
    parts = [TopicPartition(topic, p, 0) for p in meta.topics[topic].partitions]
    fins = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in parts}
    c.assign(parts)
    restant = {p: f for p, f in fins.items() if f > 0}
    while restant:
        m = c.poll(2.0)
        if m is None:
            continue
        if m.error():
            continue
        yield m.value()
        if m.offset() + 1 >= restant.get(m.partition(), 0):
            restant.pop(m.partition(), None)
    c.close()


def main():
    direct = defaultdict(lambda: [0, 0.0])
    for v in lire_tout("commandes"):
        try:
            e = json.loads(v)
        except ValueError:
            continue
        if "ts" not in e or e.get("amount", 0) <= 0:
            continue  # messages du TP1 sans ts, ou montants invalides
        minute = e["ts"][:16]  # "AAAA-MM-JJ HH:MM"
        direct[minute][0] += 1
        direct[minute][1] += e["amount"]

    flink = defaultdict(list)
    for v in lire_tout("ca_minute"):
        r = json.loads(v)
        flink[r["w_start"][:16]].append((r["nb"], r["total"]))

    print(f"{'minute':<17} | {'Flink nb':>8} {'Flink total':>13} | {'direct nb':>9} {'direct total':>13} | écart")
    for minute in sorted(flink):
        for nb, total in flink[minute]:
            dn, dt = direct.get(minute, (0, 0.0))
            ecart = "identique" if nb == dn and abs(total - dt) < 0.01 else f"{nb - dn:+d} cmd, {total - dt:+.2f}"
            print(f"{minute:<17} | {nb:>8} {total:>13.2f} | {dn:>9} {dt:>13.2f} | {ecart}")


if __name__ == "__main__":
    main()
