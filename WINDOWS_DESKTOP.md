# 🪟 JBAFI - Windows Desktop (7 Contenedores)

**Guía completa para ejecutar los 7 contenedores en tu Windows con Docker Desktop**

---

## 📋 Requisitos

- **Windows 10/11** con WSL 2
- **Docker Desktop** instalado
- **Git** instalado
- **2-4 GB de RAM** disponible (mínimo 8GB total)

### Instalar Docker Desktop

1. Descargar: https://www.docker.com/products/docker-desktop
2. Ejecutar instalador
3. Seguir pasos (marcar "Install WSL 2" si aparece)
4. Reiniciar Windows
5. Abrir Docker Desktop desde Inicio
6. Esperar a que diga "Docker is running"

---

## 🚀 Inicio Rápido (3 pasos)

### Paso 1: Clonar desde GitHub

**Abre PowerShell:**

```powershell
git clone https://github.com/TU-USUARIO/jbafi.git
cd jbafi
```

### Paso 2: Ejecutar setup.bat

**Doble click en `setup.bat`** o en PowerShell:

```powershell
.\setup.bat
```

Selecciona opción: **1** (Iniciar 7 contenedores)

### Paso 3: Acceder

Abre tu navegador:

| Servicio | URL |
|----------|-----|
| **Frontend** | http://localhost:3000 |
| **Backend Principal** | http://localhost:8000 |
| **Backend 926** | http://localhost:8011 |
| **Documentación API** | http://localhost:8000/docs |

---

## 📊 Los 7 Contenedores

```
1. afi_frontend_nova      → Puerto 3000   (Interfaz web React)
2. afi_backend            → Puerto 8000   (Lógica principal Imagine)
3. afi_postgres           → Puerto 5432   (BD principal)
4. afi_qdrant             → Puerto 6333   (Vector store búsquedas)
5. afi_ollama             → Puerto 11434  (Modelo LLM local)
6. clone_full_backend     → Puerto 8011   (Motor compatibilidad 926)
7. clone_full_postgres    → Puerto 5433   (BD compatibilidad 926)
```

---

## 🔧 Comandos Útiles

**En PowerShell:**

```powershell
# Ver estado de todos
docker-compose ps

# Ver logs
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f afi_backend

# Reiniciar un servicio
docker-compose restart afi_backend

# Detener todos
docker-compose down

# Limpiar todo (borra datos)
docker-compose down -v
```

---

## ⚠️ Problemas Comunes en Windows

### ❌ "docker: command not found"
**Solución:** Docker Desktop no está en PATH
- Reinstala Docker Desktop
- Reinicia PowerShell/Windows Terminal
- Verifica: `docker --version`

### ❌ "Cannot connect to Docker daemon"
**Solución:** Docker Desktop no está corriendo
1. Abre Docker Desktop desde Inicio
2. Espera a que diga "Docker is running"
3. Intenta de nuevo

### ❌ "Port 3000 already in use"
**Solución 1:** Detener otros servicios en puerto 3000

**Solución 2:** Cambiar puerto en `docker-compose.yml`:
```yaml
afi_frontend_nova:
  ports:
    - "3001:5173"  # Cambiar 3000 a 3001
```

### ❌ "WSL 2 installation is incomplete"
**Solución:**
```powershell
# En PowerShell como administrador
wsl --update
```
Luego reinicia Windows.

### ❌ "Out of memory"
**Solución:** Aumentar memoria en Docker Desktop
1. Docker Desktop → Settings → Resources
2. Sube "Memory" a 8-12GB
3. Reinicia Docker

---

## 🔄 Flujo Típico de Desarrollo

```powershell
# 1. Clonar (primera vez)
git clone https://github.com/TU-USUARIO/jbafi.git
cd jbafi

# 2. Iniciar
.\setup.bat
# Elige opción 1

# 3. En otra ventana PowerShell, hacer cambios en código
# Los cambios se reflejan automáticamente (hot reload)

# 4. Ver logs si hay errores
docker-compose logs -f

# 5. Cuando termines
docker-compose down
```

---

## 💾 Backup de Datos

Los datos se guardan en volúmenes Docker:

```powershell
# Ver volúmenes
docker volume ls | findstr "afi_\|clone_"

# Los datos están en Windows en:
# C:\ProgramData\Docker\volumes\
```

Para backup manual:

```powershell
# Backup BD principal
docker exec afi_postgres pg_dump -U afi_user -d afiliaciones > backup_afi.sql

# Backup BD 926
docker exec clone_full_postgres pg_dump -U admin -d migracion > backup_clone.sql
```

---

## 🌐 Acceder desde Otro Dispositivo

**En tu red local (mismo WiFi):**

1. Encuentra tu IP Windows:
```powershell
ipconfig
```
Busca "IPv4 Address" (ej: `192.168.1.100`)

2. Desde otro dispositivo accede a:
```
http://192.168.1.100:3000
```

---

## ✅ Checklist

- [ ] Docker Desktop instalado y corriendo
- [ ] Git instalado
- [ ] Repositorio clonado
- [ ] setup.bat ejecutado
- [ ] Opción 1 seleccionada
- [ ] Frontend: http://localhost:3000 ✅
- [ ] Backend: http://localhost:8000/docs ✅
- [ ] Todos los 7 en: `docker-compose ps`

---

## 📈 Rendimiento

### Primera ejecución
- Descarga imágenes: 5-10 minutos
- Instala dependencias (npm, pip): 2-3 minutos
- **Total:** 10-15 minutos

### Ejecuciones posteriores
- `docker-compose up`: < 1 minuto ⚡

---

## 🎯 Próximos Pasos

**Desarrollo:**
```powershell
# Editar código en ./backend o ./frontend-nova
# Los cambios se reflejan al guardar (hot reload)
```

**Contribuir:**
```powershell
git add .
git commit -m "Feature: descripción"
git push
```

---

## 🆘 Si Aún Hay Problemas

1. **Lee los logs:**
   ```powershell
   docker-compose logs -f
   ```

2. **Reinicia todo:**
   ```powershell
   docker-compose down
   docker-compose up
   ```

3. **Contacta:** [correo/teléfono del equipo]

---

## 📞 Soporte Rápido

| Problema | Comando |
|----------|---------|
| Ver estado | `docker-compose ps` |
| Ver logs | `docker-compose logs -f` |
| Reiniciar | `docker-compose restart` |
| Detener | `docker-compose down` |
| Acceder shell | `docker exec -it afi_backend bash` |

---

**¡Sistema 100% funcional en Windows Desktop!** 🎉

Lee JBAFI_README.md para información general.
