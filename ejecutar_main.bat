@echo off
setlocal
REM --- Ir dinámicamente al directorio donde reside este script ---
cd /d "%~dp0"

REM --- Crear carpeta logs si no existe ---
if not exist "logs" mkdir "logs"

REM --- Detectar intérprete de Python ---
if exist "env\Scripts\python.exe" (
    set "PYTHON_CMD=env\Scripts\python.exe"
) else (
    set "PYTHON_CMD=python"
)

REM --- Ejecutar el pipeline principal ---
echo [%date% %time%] Iniciando ejecucion del pipeline ETL...
%PYTHON_CMD% main.py
set "EXIT_CODE=%errorlevel%"

REM --- Registrar salida para monitoreo / tareas programadas ---
echo [%date% %time%] Pipeline finalizado con codigo %EXIT_CODE% >> "logs\log_tarea.txt"

exit /b %EXIT_CODE%
