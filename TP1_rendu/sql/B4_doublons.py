#!/usr/bin/env python3
"""B4 : y a-t-il des doublons dans alertes_exam ?
Une alerte est identifiée par (customer_id, w_start) : un client ne peut avoir qu'une alerte par minute.
On relit tout le topic et on compte les clés présentes plusieurs fois, minute par minute.

Usage : python B4_doublons.py [topic] [isolation]
        python B4_doublons.py alertes_exam
        python B4_doublons.py alertes_exam_eos read_committed
"""
import json
import sys
from collections import Counter
from confluent_kafka import Consumer, TopicPartition

TOPIC = sys.argv[1] if len(sys.argv) > 1 else "alertes_exam"
ISOLATION = sys.argv[2] if len(sys.argv) > 2 else "read_uncommitted"

c = Consumer({"bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
              "group.id": "verif-doublons-b4", "auto.offset.reset": "earliest", "enable.auto.commit": False,
              "isolation.level": ISOLATION})
parts = [TopicPartition(TOPIC, p, 0) for p in c.list_topics(TOPIC).topics[TOPIC].partitions]
fins = {p.partition: c.get_watermark_offsets(p)[1] for p in parts}
c.assign(parts)
cles, lus, vides = Counter(), {p: 0 for p in fins}, 0
# en read_committed, les messages des transactions annulées et les marqueurs de transaction
# ne sont jamais livrés : on s'arrête après quelques secondes sans nouveau message
while any(lus[p] < fins[p] for p in fins) and vides < 5:
    m = c.poll(1.0)
    if m is None:
        vides += 1
        continue
    if m.error():
        continue
    vides = 0
    lus[m.partition()] = m.offset() + 1
    a = json.loads(m.value())
    cles[(a["customer_id"], a["w_start"])] += 1
c.close()

print(f"topic {TOPIC}, isolation.level={ISOLATION}")
print(f"{'minute':<20} {'alertes reçues':>14} {'alertes uniques':>15} {'doublons':>9}")
for w in sorted({w for _, w in cles}):
    recues = sum(n for (_, ws), n in cles.items() if ws == w)
    uniques = sum(1 for (_, ws) in cles if ws == w)
    print(f"{w:<20} {recues:>14} {uniques:>15} {recues - uniques:>9}")
total, uniques = sum(cles.values()), len(cles)
print(f"{'TOTAL':<20} {total:>14} {uniques:>15} {total - uniques:>9}")
