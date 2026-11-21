"""Create independent synthetic input for build-time compiler profiling."""
import csv
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path(sys.argv[1])
for name in ("events", "clicks", "links"):
    (root / name).mkdir(parents=True, exist_ok=True)

randomizer = random.Random(321)
start = datetime(2025, 1, 6, tzinfo=timezone.utc)
end = start + timedelta(days=14)
alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"


def f1(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp(value, choice):
    if choice == 0:
        return str(int(value.timestamp() * 1000))
    if choice == 1:
        offset = timezone(timedelta(hours=5, minutes=30))
        return value.astimezone(offset).isoformat(timespec="seconds")
    return f1(value)


def key(prefix, number):
    chars = []
    while number:
        number, digit = divmod(number, len(alphabet))
        chars.append(alphabet[digit])
    return prefix + ("".join(reversed(chars)) or "0")


def writer(name, header):
    handle = (root / name).open("w", newline="")
    rows = csv.writer(handle, lineterminator="\n")
    rows.writerow(header)
    return handle, rows


def sampled_time(begin, seconds):
    return begin + timedelta(seconds=randomizer.randrange(seconds))


(root / "manifest.json").write_text(json.dumps({"window_start": f1(start), "window_end": f1(end)}))
people = [key("u", randomizer.randrange(36**8)) for _ in range(20000)]
people[0] = "mixed-case-identifier"

handle, rows = writer("links/part.csv", ["user_id_a", "user_id_b"])
with handle:
    for i in range(3000):
        rows.writerow([people[i], people[i // 9]])

handle, rows = writer("products.csv", ["product_id", "category"])
with handle:
    for i in range(4000):
        rows.writerow([f"p{i}", f"category-{i % 20}"])

handle, rows = writer("campaigns.csv", ["campaign_id", "channel"])
with handle:
    for i in range(2000):
        rows.writerow([f"c_{i}", f"channel-{i % 6}"])

handle, rows = writer(
    "events/part.csv", ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
)
with handle:
    for i in range(250000):
        time = sampled_time(start, 14 * 86400)
        lag = randomizer.randrange(1000)
        if i % 97 == 0:
            lag += 86400
        stamp = "bad-date" if i % 123 == 0 else timestamp(time, i % 7)
        event_type = "purchase" if i % 31 == 0 else ("cart" if i % 6 == 0 else "view")
        user = people[i % 12] if i % 10 == 0 else randomizer.choice(people)
        event_id = key("e", (i * 1315423911 + 17) % 36**8)
        event = [event_id, user, stamp, f1(time + timedelta(seconds=lag)), event_type,
                 f"p{i % 4000}", str(i % 50000 + 1) if event_type == "purchase" else ""]
        if i % 9 == 0:
            handle.write(",".join(f'"{value}"' for value in event) + "\n")
        else:
            rows.writerow(event)
        if i % 27 == 0:
            event[3] = f1(time + timedelta(seconds=lag + 1))
            rows.writerow(event)

handle, rows = writer("clicks/part.csv", ["click_id", "user_id", "campaign_id", "ts"])
with handle:
    for i in range(75000):
        time = sampled_time(start - timedelta(days=7), 21 * 86400)
        user = people[i % 12] if i % 12 == 0 else randomizer.choice(people)
        rows.writerow([key("k", (i * 2654435761 + 97) % 36**8), user,
                       f"c_{i % 2000}", timestamp(time, i % 7)])
