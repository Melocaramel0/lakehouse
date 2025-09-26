from pathlib import Path
import pandas as pd
import plotly.express as px

BASE = Path(__file__).resolve().parents[1]
prepared_dir = BASE / "data" / "prepared"
docs_dir = BASE / "docs"
docs_dir.mkdir(parents=True, exist_ok=True)

by_user_file = prepared_dir / "by_user.parquet"
if not by_user_file.exists():
    print("No existe data/prepared/by_user.parquet. Corre primero prepare_analytics_pandas.py")
    raise SystemExit(0)

# Cargar top usuarios
df_user = pd.read_parquet(by_user_file)
df_user = df_user.sort_values("total_amount", ascending=False).head(10)

# Guardar métricas (para el informe)
out_csv = docs_dir / "metrics_summary.csv"
df_user.to_csv(out_csv, index=False)

# Gráfico interactivo
fig = px.bar(
    df_user,
    x="user_id",
    y="total_amount",
    title="Top 10 usuarios por monto total",
    labels={"user_id": "Usuario", "total_amount": "Monto total"},
)
fig.update_layout(xaxis_tickangle=60)
out_html = docs_dir / "top_users.html"
fig.write_html(out_html, include_plotlyjs="cdn")

print(f"OK: {out_csv} y {out_html} generados.")
