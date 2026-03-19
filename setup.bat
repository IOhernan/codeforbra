@echo off
REM ============================================================
REM JBAFI - Setup Script para Windows
REM 7 Contenedores: Sistema Completo AFI
REM ============================================================

echo.
echo ============================================================
echo   JBAFI - 7 Contenedores (Windows Desktop)
echo ============================================================
echo.

REM Verificar Docker
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no está instalado
    echo.
    echo Descarga Docker Desktop:
    echo https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

echo [OK] Docker detectado
docker --version

REM Verificar Docker Compose
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Compose no está disponible
    pause
    exit /b 1
)

docker-compose --version
echo.

REM Verificar Docker está corriendo
docker ps >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no está corriendo
    echo.
    echo Por favor abre Docker Desktop y espera a que diga:
    echo "Docker is running"
    echo.
    pause
    exit /b 1
)

echo [OK] Docker está corriendo
echo.

REM Crear estructura de carpetas si no existe
echo [INFO] Verificando estructura de carpetas...
if not exist "frontend-nova" (
    call init.bat
)
echo.

REM Menu
echo ============================================================
echo OPCIONES:
echo ============================================================
echo 1. Iniciar 7 contenedores (docker-compose up)
echo 2. Detener contenedores (docker-compose down)
echo 3. Ver logs (docker-compose logs -f)
echo 4. Reiniciar contenedores
echo 5. Limpiar todo (docker-compose down -v)
echo 6. Salir
echo.

set /p choice="Elige opción (1-6): "

if "%choice%"=="1" (
    echo.
    echo [INFO] Iniciando los 7 contenedores...
    echo Primera ejecución puede tardar 5-10 minutos
    echo.
    docker-compose up -d
    echo.
    echo [OK] Contenedores iniciados en background
    echo Verifica con: docker ps
    goto end
)

if "%choice%"=="2" (
    echo.
    echo [INFO] Deteniendo contenedores...
    docker-compose down
    echo [OK] Contenedores detenidos
    goto end
)

if "%choice%"=="3" (
    echo.
    echo [INFO] Mostrando logs (Presiona Ctrl+C para salir)
    docker-compose logs -f
    goto end
)

if "%choice%"=="4" (
    echo.
    echo [INFO] Reiniciando contenedores...
    docker-compose restart
    echo [OK] Contenedores reiniciados
    goto end
)

if "%choice%"=="5" (
    echo.
    echo [ADVERTENCIA] Se eliminarán TODOS los datos
    set /p confirm="¿Estás seguro? (s/n): "
    if /i "%confirm%"=="s" (
        echo [INFO] Eliminando todo...
        docker-compose down -v
        echo [OK] Limpieza completada
    ) else (
        echo [CANCELADO]
    )
    goto end
)

if "%choice%"=="6" (
    echo Saliendo...
    goto end
)

echo [ERROR] Opción no válida
goto end

:end
echo.
echo ============================================================
echo ACCESO A LOS SERVICIOS:
echo ============================================================
echo.
echo Frontend:         http://localhost:3000
echo Backend:          http://localhost:8000
echo Backend 926:      http://localhost:8011
echo Backend Docs:     http://localhost:8000/docs
echo Qdrant:           http://localhost:6333
echo.
echo ============================================================
echo.
pause
