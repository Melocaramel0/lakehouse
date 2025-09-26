@echo off
set PYEXE=.\.venv\Scripts\python.exe
if not exist "%PYEXE%" (
  py -3.11 -m venv .venv
  set PYEXE=.\.venv\Scripts\python.exe
)

"%PYEXE%" -m pip install --upgrade pip setuptools wheel
"%PYEXE%" -m pip install pandas==2.2.2 pyarrow==16.1.0 plotly==5.23.0 kaleido==0.2.1

"%PYEXE%" .\src\generate_raw.py  || goto :eof
"%PYEXE%" .\src\transform_to_parquet_pandas.py  || goto :eof
"%PYEXE%" .\src\prepare_analytics_pandas.py  || goto :eof
"%PYEXE%" .\src\queries_example.py  || goto :eof

echo ✅ Listo. Revisa la carpeta docs y data.
pause
