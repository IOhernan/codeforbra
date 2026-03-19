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

REM Crear archivos package.json para frontend
(
echo {
echo   "name": "afi-frontend",
echo   "version": "1.0.0",
echo   "type": "module",
echo   "scripts": {
echo     "dev": "vite",
echo     "build": "vite build",
echo     "preview": "vite preview"
echo   },
echo   "dependencies": {
echo     "react": "^18.0.0",
echo     "react-dom": "^18.0.0"
echo   },
echo   "devDependencies": {
echo     "vite": "^5.0.0"
echo   }
echo }
) > frontend-nova\package.json

echo.
echo [OK] Estructura de carpetas creada exitosamente
echo.
echo Los Dockerfiles ya están en el repositorio.
echo.
echo Próximo paso: ejecuta setup.bat nuevamente
echo.
pause
