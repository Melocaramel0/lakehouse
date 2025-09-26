from pathlib import Path
import pandas as pd
import glob

BASE = Path(__file__).resolve().parents[1]
raw_dir = BASE / "data" / "raw"
processed_dir = BASE / "data" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

# Leer todos los CSV de data/raw/*/*.csv
files = glob.glob(str(raw_dir / "*" / "*.csv"))
if not files:
    print("No hay CSV en data/raw.")
    raise SystemExit(0)

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

# Cast & clean
df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
df = df.dropna(subset=["event_time", "user_id", "category", "amount"])
df["event_date"] = df["event_time"].dt.date.astype(str)

# Guardar en Parquet "particionado" por event_date (carpetas)
for date_value, g in df.groupby("event_date"):
    out_dir = processed_dir / f"event_date={date_value}"
    out_dir.mkdir(parents=True, exist_ok=True)
    g.to_parquet(out_dir / "part-0000.parquet", index=False)

print(f"Parquet escrito en {processed_dir}")
