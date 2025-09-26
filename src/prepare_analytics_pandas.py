from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
processed_dir = BASE / "data" / "processed"
prepared_dir = BASE / "data" / "prepared"
prepared_dir.mkdir(parents=True, exist_ok=True)

# Leer todos los parquet de processed (recursivo)
files = list(processed_dir.rglob("*.parquet"))
if not files:
    print("No hay Parquet en processed.")
    raise SystemExit(0)

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

by_user = (df.groupby("user_id", as_index=False)
             .agg(events=("user_id","count"),
                  total_amount=("amount","sum")))
by_cat = (df.groupby("category", as_index=False)
            .agg(events=("category","count"),
                 total_amount=("amount","sum")))

by_user.to_parquet(prepared_dir / "by_user.parquet", index=False)
by_cat.to_parquet(prepared_dir / "by_category.parquet", index=False)

print(f"Datasets analíticos en {prepared_dir}")
