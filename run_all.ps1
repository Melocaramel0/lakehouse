param(
  [string]$PythonExe = ".\.venv\Scripts\python.exe"
)

# Activar venv si no está activo
if (-not (Test-Path $PythonExe)) {
  Write-Host "No existe .venv o no es 3.11. Creándolo..."
  py -3.11 -m venv .venv
  $PythonExe = ".\.venv\Scripts\python.exe"
}

# Mostrar intérprete y versión
Write-Host "Usando Python:" (Resolve-Path $PythonExe)
& $PythonExe --version

# Asegurar dependencias mínimas
& $PythonExe -m pip install --upgrade pip setuptools wheel
& $PythonExe -m pip install pandas==2.2.2 pyarrow==16.1.0

# Para el gráfico (opcional pero recomendado)
# Si ya lo instalaste, no pasa nada; si no, lo instala.
& $PythonExe -m pip install plotly==5.23.0
& $PythonExe -m pip install kaleido==0.2.1

# Ejecutar el pipeline (versión pandas)
& $PythonExe .\src\generate_raw.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $PythonExe .\src\transform_to_parquet_pandas.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $PythonExe .\src\prepare_analytics_pandas.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $PythonExe .\src\queries_example.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "✅ Pipeline completado. Revisa:"
Write-Host " - data\processed\event_date=YYYY-MM-DD\part-0000.parquet"
Write-Host " - data\prepared\by_user.parquet y by_category.parquet"
Write-Host " - docs\metrics_summary.csv, docs\top_users.html y (si kaleido) docs\top_users.png"
