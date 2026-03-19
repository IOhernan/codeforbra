@echo off
REM ============================================================
REM JBAFI - Inicializar estructura de carpetas
REM ============================================================

echo.
echo [INFO] Creando estructura de carpetas necesarias...
echo.

REM Crear carpetas principales
mkdir frontend-nova 2>nul
mkdir backend 2>nul
mkdir migracion_py_react\backend 2>nul
mkdir data 2>nul

REM Crear archivos dummy para Docker (evita errores de contexto vacío)
echo # Frontend Nova > frontend-nova\package.json
echo {} > backend\Dockerfile
echo # Migracion Backend > migracion_py_react\backend\Dockerfile

echo.
echo [OK] Estructura de carpetas creada exitosamente
echo.
echo Ahora ejecuta: docker-compose up -d
echo.
pause
