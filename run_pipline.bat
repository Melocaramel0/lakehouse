@echo off
setlocal

set PYEXE=.\.venv\Scripts\python.exe

if not exist "%PYEXE%" (
  echo ❌ No se encontró el entorno .venv. Actívalo o créalo con: py -3.11 -m venv .venv
  pause
  exit /b
)

echo ▶ Ejecutando generate_raw.py ...
"%PYEXE%" .\src\generate_raw.py  || goto :error

echo ▶ Ejecutando transform_to_parquet_pandas.py ...
"%PYEXE%" .\src\transform_to_parquet_pandas.py  || goto :error

echo ▶ Ejecutando prepare_analytics_pandas.py ...
"%PYEXE%" .\src\prepare_analytics_pandas.py  || goto :error

echo ▶ Ejecutando queries_example.py ...
"%PYEXE%" .\src\queries_example.py  || goto :error

echo.
echo ✅ Pipeline completado. Revisa las carpetas:
echo    - data\processed\
echo    - data\prepared\
echo    - docs\metrics_summary.csv, docs\top_users.html (y PNG si usas kaleido)
pause
exit /b

:error
echo ❌ Ocurrió un error en el paso anterior.
pause
exit /b
