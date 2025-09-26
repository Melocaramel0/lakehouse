\
import csv, os, random
from pathlib import Path
from datetime import datetime, timedelta

BASE = Path(__file__).resolve().parents[1]
raw_dir = BASE / "data" / "raw"

# Generate data for today and yesterday
today = datetime.now().date()
dates = [today - timedelta(days=d) for d in (0, 1)]

users = [f"user_{i:03d}" for i in range(1, 51)]
categories = ["electronics", "grocery", "fashion", "sports", "books"]

for d in dates:
    day_dir = raw_dir / d.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    file_path = day_dir / f"transactions_{d.strftime('%Y%m%d')}.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_time","user_id","category","amount"])
        for _ in range(200):
            ts = datetime(d.year, d.month, d.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))
            row = [
                ts.isoformat(sep=" "),
                random.choice(users),
                random.choice(categories),
                round(random.uniform(1.0, 500.0), 2)
            ]
            writer.writerow(row)
print(f"Generated sample CSVs under {raw_dir}")
