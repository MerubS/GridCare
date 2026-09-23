#!/usr/bin/env python3
"""
GridCare simulator
------------------
Streams synthetic smart-meter, weather, grid-status, and household data for a
Houston heat-wave scenario into Confluent Cloud (Avro + Schema Registry).

Scripted events, so the demo always has something to show:
  * 14:00 sim time  -> power outage in ZIP 77021 until 17:30
  * 15:00 sim time  -> AC failure in 3 medical-baseline homes outside 77021

Time is compressed: by default 1 simulated minute = 1 real second, so a full
6 AM -> 10 PM day runs in about 16 minutes.

Setup:
  pip install -r requirements.txt
  copy .env.example to .env and fill in your Confluent Cloud keys
  (the .env file is git-ignored, so your keys never reach GitHub)

Run (from the repo root):
  python simulator/gridcare_simulator.py --dry-run          # test locally, no Kafka
  python simulator/gridcare_simulator.py --households 2000  # real run

Re-running: Flink uses event time, so re-sending the same simulated day makes
the new events "late". For a second run, pass a new date, e.g.
  python gridcare_simulator.py --sim-date 2026-09-24
"""
import argparse
import json
import math
import os
import random
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

HOUSTON = ZoneInfo("America/Chicago")
ZIPS = ["77002", "77004", "77006", "77007", "77008", "77009", "77011", "77012",
        "77016", "77020", "77021", "77026", "77028", "77033", "77051", "77087"]
OUTAGE_ZIP = "77021"
OUTAGE_START, OUTAGE_END = 14 * 60, 17 * 60 + 30   # minutes after midnight
AC_FAILURE_START = 15 * 60
MEDICAL_DEVICES = ["OXYGEN_CONCENTRATOR", "CPAP", "HOME_DIALYSIS", "POWER_WHEELCHAIR"]
FIRST = ["Maria", "James", "Linda", "Robert", "Patricia", "Willie", "Dorothy",
         "Jose", "Betty", "Charles", "Gloria", "Earl", "Rosa", "Harold"]
LAST = ["Johnson", "Garcia", "Williams", "Nguyen", "Brown", "Hernandez",
        "Jackson", "Lee", "Davis", "Martinez", "Thomas", "Robinson"]


def ts(name):
    return {"name": name, "type": {"type": "long", "logicalType": "timestamp-millis"}}


SCHEMAS = {
    "households": {
        "type": "record", "name": "Household", "namespace": "gridcare",
        "doc": "Opt-in medical baseline / vulnerable customer registry",
        "fields": [
            {"name": "household_id", "type": "string"},
            {"name": "meter_id", "type": "string"},
            {"name": "zip", "type": "string"},
            {"name": "medical_baseline", "type": "boolean"},
            {"name": "medical_device", "type": "string", "doc": "PII-sensitive"},
            {"name": "age_65_plus", "type": "boolean"},
            {"name": "contact_name", "type": "string", "doc": "PII"},
            {"name": "contact_phone", "type": "string", "doc": "PII"},
            ts("updated_ts"),
        ],
    },
    "meter_readings": {
        "type": "record", "name": "MeterReading", "namespace": "gridcare",
        "fields": [
            {"name": "meter_id", "type": "string"},
            {"name": "zip", "type": "string"},
            {"name": "kw", "type": "double", "doc": "Average demand over the interval, kW"},
            ts("reading_ts"),
        ],
    },
    "weather_obs": {
        "type": "record", "name": "WeatherObservation", "namespace": "gridcare",
        "fields": [
            {"name": "zip", "type": "string"},
            {"name": "temp_f", "type": "double"},
            {"name": "humidity_pct", "type": "double"},
            {"name": "heat_index_f", "type": "double"},
            ts("obs_ts"),
        ],
    },
    "grid_status": {
        "type": "record", "name": "GridStatus", "namespace": "gridcare",
        "fields": [
            {"name": "zip", "type": "string"},
            {"name": "status", "type": "string", "doc": "NORMAL or OUTAGE"},
            ts("status_ts"),
        ],
    },
}


def heat_index_f(t, rh):
    """NWS heat index (Rothfusz regression, with the simple formula below 80F)."""
    simple = 0.5 * (t + 61.0 + (t - 68.0) * 1.2 + rh * 0.094)
    if (simple + t) / 2 < 80:
        return simple
    return (-42.379 + 2.04901523 * t + 10.14333127 * rh - 0.22475541 * t * rh
            - 0.00683783 * t * t - 0.05481717 * rh * rh + 0.00122874 * t * t * rh
            + 0.00085282 * t * rh * rh - 0.00000199 * t * t * rh * rh)


class Home:
    def __init__(self, i, rng):
        self.household_id = f"HH-{i:06d}"
        self.meter_id = f"MTR-{i:06d}"
        self.zip = rng.choice(ZIPS)
        self.medical_baseline = rng.random() < 0.15
        self.medical_device = (rng.choice(MEDICAL_DEVICES)
                               if self.medical_baseline and rng.random() < 0.6 else "NONE")
        self.age_65_plus = rng.random() < (0.7 if self.medical_baseline else 0.2)
        self.contact_name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        self.contact_phone = f"+1-713-555-01{rng.randint(0, 99):02d}"  # fictional range
        self.base_kw = rng.uniform(0.35, 0.6)
        self.ac_eff = rng.uniform(0.09, 0.14)
        self.ac_failed = False

    def kw(self, temp_f, minute, rng):
        if self.zip == OUTAGE_ZIP and OUTAGE_START <= minute < OUTAGE_END:
            return 0.0
        if self.ac_failed and minute >= AC_FAILURE_START:
            return round(max(0.05, rng.gauss(0.15, 0.03)), 3)   # fridge only
        ac = max(0.0, temp_f - 76) * self.ac_eff
        device = 0.12 if self.medical_device != "NONE" else 0.0
        return round(max(0.05, rng.gauss(self.base_kw + ac + device, 0.08)), 3)

    def record(self, updated_ms):
        return {k: getattr(self, k) for k in (
            "household_id", "meter_id", "zip", "medical_baseline", "medical_device",
            "age_65_plus", "contact_name", "contact_phone")} | {"updated_ts": updated_ms}


class Sink:
    def __init__(self, dry_run, partitions):
        self.dry_run = dry_run
        self.counts = {t: 0 for t in SCHEMAS}
        self.errors = 0
        if dry_run:
            return
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        kafka_conf = {
            "bootstrap.servers": env("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": env("KAFKA_API_KEY"),
            "sasl.password": env("KAFKA_API_SECRET"),
        }
        ensure_topics(kafka_conf, partitions)
        sr = SchemaRegistryClient({
            "url": env("SR_URL"),
            "basic.auth.user.info": f"{env('SR_API_KEY')}:{env('SR_API_SECRET')}",
        })
        self.serializers = {t: AvroSerializer(sr, json.dumps(s)) for t, s in SCHEMAS.items()}
        self.producer = Producer(kafka_conf | {
            "linger.ms": 50, "batch.size": 262144, "compression.type": "lz4",
            "queue.buffering.max.messages": 500000,
        })

    def _on_delivery(self, err, _msg):
        if err:
            self.errors += 1
            if self.errors <= 5:
                print(f"  delivery error: {err}", file=sys.stderr)

    def send(self, topic, key, value):
        self.counts[topic] += 1
        if self.dry_run:
            if self.counts[topic] == 1:
                print(f"  sample {topic}: {value}")
            return
        from confluent_kafka.serialization import MessageField, SerializationContext
        payload = self.serializers[topic](value, SerializationContext(topic, MessageField.VALUE))
        while True:
            try:
                self.producer.produce(topic, key=key.encode(), value=payload,
                                      on_delivery=self._on_delivery)
                return
            except BufferError:
                self.producer.poll(0.5)

    def poll(self):
        if not self.dry_run:
            self.producer.poll(0)

    def flush(self):
        if not self.dry_run:
            self.producer.flush(60)


def env(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Missing environment variable {name}")
    return value


def ensure_topics(kafka_conf, partitions):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(kafka_conf)
    existing = set(admin.list_topics(timeout=20).topics)
    configs = {"households": {"cleanup.policy": "compact"}}
    new = [NewTopic(t, num_partitions=partitions, replication_factor=3,
                    config=configs.get(t, {}))
           for t in SCHEMAS if t not in existing]
    for topic, fut in admin.create_topics(new).items() if new else []:
        try:
            fut.result()
            print(f"  created topic {topic}")
        except Exception as e:  # noqa: BLE001
            print(f"  could not create {topic}: {e}")


def main():
    p = argparse.ArgumentParser(description="GridCare heat-wave data simulator")
    p.add_argument("--households", type=int, default=2000)
    p.add_argument("--sec-per-sim-minute", type=float, default=1.0,
                   help="Real seconds per simulated minute (lower = faster)")
    p.add_argument("--start-hour", type=int, default=6)
    p.add_argument("--end-hour", type=int, default=22)
    p.add_argument("--sim-date", type=date.fromisoformat, default=date.today())
    p.add_argument("--partitions", type=int, default=6)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dry-run", action="store_true", help="Generate data without Kafka")
    args = p.parse_args()

    rng = random.Random(args.seed)
    midnight_ms = int(datetime(args.sim_date.year, args.sim_date.month, args.sim_date.day,
                               tzinfo=HOUSTON).timestamp() * 1000)
    homes = [Home(i, rng) for i in range(args.households)]
    zip_heat_island = {z: rng.uniform(-1.5, 2.0) for z in ZIPS}

    candidates = [h for h in homes if h.medical_baseline and h.zip != OUTAGE_ZIP]
    for h in rng.sample(candidates, k=min(3, len(candidates))):
        h.ac_failed = True

    vulnerable = sum(h.medical_baseline for h in homes)
    print(f"GridCare: {len(homes)} homes, {vulnerable} medical-baseline, "
          f"{len(ZIPS)} ZIPs, sim date {args.sim_date}")
    print("Scripted AC failures (these should become INDIVIDUAL_COOLING_LOSS alerts):")
    for h in homes:
        if h.ac_failed:
            print(f"  {h.household_id} / {h.meter_id} in {h.zip}, device={h.medical_device}")
    in_outage = sum(h.medical_baseline and h.zip == OUTAGE_ZIP for h in homes)
    print(f"Medical-baseline homes inside outage ZIP {OUTAGE_ZIP}: {in_outage}")

    sink = Sink(args.dry_run, args.partitions)
    start_ms = midnight_ms + args.start_hour * 3600_000
    for h in homes:
        sink.send("households", h.meter_id, h.record(start_ms))
    sink.flush()

    next_tick = time.monotonic()
    for minute in range(args.start_hour * 60, args.end_hour * 60):
        now_ms = midnight_ms + minute * 60_000
        s = math.sin(math.pi * (minute / 60 - 9) / 12)
        temps = {}
        for z in ZIPS:
            t = 86 + 10 * s + zip_heat_island[z] + rng.gauss(0, 0.3)
            rh = min(95.0, max(30.0, 75 - 25 * s + rng.gauss(0, 2)))
            temps[z] = t
            sink.send("weather_obs", z, {"zip": z, "temp_f": round(t, 1),
                                         "humidity_pct": round(rh, 1),
                                         "heat_index_f": round(heat_index_f(t, rh), 1),
                                         "obs_ts": now_ms})
            outage = z == OUTAGE_ZIP and OUTAGE_START <= minute < OUTAGE_END
            sink.send("grid_status", z, {"zip": z, "status": "OUTAGE" if outage else "NORMAL",
                                         "status_ts": now_ms})
        for h in homes:
            sink.send("meter_readings", h.meter_id,
                      {"meter_id": h.meter_id, "zip": h.zip,
                       "kw": h.kw(temps[h.zip], minute, rng), "reading_ts": now_ms})
        sink.poll()

        if minute == OUTAGE_START:
            print(f"  >> {minute // 60:02d}:{minute % 60:02d} OUTAGE begins in {OUTAGE_ZIP}")
        if minute == AC_FAILURE_START:
            print(f"  >> {minute // 60:02d}:{minute % 60:02d} AC failures begin")
        if minute == OUTAGE_END:
            print(f"  >> {minute // 60:02d}:{minute % 60:02d} power restored in {OUTAGE_ZIP}")
        if minute % 60 == 0:
            print(f"  sim {minute // 60:02d}:00  sent {sum(sink.counts.values()):,} events")

        if not args.dry_run:
            next_tick += args.sec_per_sim_minute
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)

    sink.flush()
    print(f"Done. Events per topic: {sink.counts}. Delivery errors: {sink.errors}")


if __name__ == "__main__":
    main()
