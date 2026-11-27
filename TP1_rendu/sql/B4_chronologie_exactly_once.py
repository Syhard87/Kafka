#!/usr/bin/env python3
"""B4 : chronologie d'une fenêtre en exactly-once.

Toutes les 50 ms, on lit pour le topic alertes_exam_eos :
  - le high watermark (HW)     : messages physiquement écrits, transaction ouverte ou non
                                  (ListOffsets en isolation read_uncommitted)
  - le last stable offset (LSO): ce qu'un lecteur read_committed peut lire
                                  (ListOffsets en isolation read_committed)
et on note la fin de chaque checkpoint Flink. On voit ainsi l'écriture (HW) puis le commit (LSO).

Attention : librdkafka lit en read_committed PAR DÉFAUT ; il faut le préciser pour obtenir le HW.
"""
import datetime as dt
import json
import time
import urllib.request

from confluent_kafka import Consumer, TopicPartition

BOOT = "localhost:19092,localhost:29092,localhost:39092"
TOPIC = "alertes_exam_eos"
parts = [TopicPartition(TOPIC, p) for p in range(3)]
hw_c = Consumer({"bootstrap.servers": BOOT, "group.id": "chrono-hw", "isolation.level": "read_uncommitted"})
lso_c = Consumer({"bootstrap.servers": BOOT, "group.id": "chrono-lso", "isolation.level": "read_committed"})
total = lambda c: sum(c.get_watermark_offsets(p, cached=False)[1] for p in parts)
get = lambda u: json.loads(urllib.request.urlopen("http://localhost:8081" + u).read())
J = [j["jid"] for j in get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"][0]
hms = lambda t: t.strftime("%H:%M:%S.%f")[:-3]


def dernier_ckpt():
    c = get(f"/jobs/{J}/checkpoints")["latest"].get("completed") or {}
    return c.get("id"), c.get("latest_ack_timestamp")


hw, lso, ck = total(hw_c), total(lso_c), dernier_ckpt()
print(f"{hms(dt.datetime.now(dt.UTC))}  départ : HW={hw}  LSO={lso}  dernier checkpoint n°{ck[0]}")
fin = time.time() + 75
evenements = 0
while time.time() < fin and evenements < 6:
    h, l, c = total(hw_c), total(lso_c), dernier_ckpt()
    t = hms(dt.datetime.now(dt.UTC))
    if h != hw:
        print(f"{t}  HW  {hw} -> {h}  : alertes ÉCRITES dans le topic (transaction ouverte, invisibles en read_committed)")
        evenements += 1
    if c[0] != ck[0]:
        fin_ck = hms(dt.datetime.fromtimestamp(c[1] / 1000, dt.UTC))
        print(f"{t}  checkpoint n°{c[0]} terminé à {fin_ck}")
    if l != lso:
        print(f"{t}  LSO {lso} -> {l}  : transaction VALIDÉE, alertes visibles en read_committed")
        evenements += 1
    hw, lso, ck = h, l, c
    time.sleep(0.05)
