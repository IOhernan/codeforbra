@echo off
REM ============================================================
REM JBAFI - Limpiar y reiniciar
REM ============================================================

echo.
echo [ADVERTENCIA] Esto detendrá todos los contenedores
echo y eliminará las carpetas de build
echo.
set /p confirm="¿Continuar? (s/n): "
if /i not "%confirm%"=="s" (
    echo [CANCELADO]
    pause
    exit /b 0
)

echo.
echo [INFO] Deteniendo contenedores...
docker-compose down -v 2>nul

echo [INFO] Eliminando carpetas...
rmdir /s /q frontend-nova 2>nul
rmdir /s /q backend 2>nul
rmdir /s /q migracion_py_react 2>nul
rmdir /s /q data 2>nul

echo.
echo [OK] Limpieza completada
echo.
echo Ahora ejecuta: init.bat
echo.
pause
