# 🚀 DEPLOYMENT A SERVIDOR - JBAFI (7 Contenedores)

## 📋 Requisitos del Servidor

### Hardware Mínimo
- **CPU:** 4 cores
- **RAM:** 16GB
- **Disco:** 100GB SSD
- **OS:** Ubuntu 20.04+ o CentOS 8+

### Software Requerido
- Docker Engine 20.10+
- Docker Compose 2.0+
- Git
- curl/wget

---

## 🚀 Instalación en Servidor Linux

### Paso 1: Instalar Docker

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Agregar usuario al grupo docker
sudo usermod -aG docker $USER
newgrp docker

# Verificar
docker --version
docker-compose --version
```

### Paso 2: Clonar Repositorio

```bash
git clone https://github.com/TU-USUARIO/jbafi.git
cd jbafi
```

### Paso 3: Configurar Variables de Entorno

```bash
# Copiar archivo de producción
cp .env.production .env

# Editar si necesario
nano .env
```

### Paso 4: Levantar los 7 Contenedores

```bash
# Usar docker-compose-production.yml
docker-compose -f docker-compose-production.yml up -d

# O si tienes alias
docker-compose up -d
```

### Paso 5: Verificar Estado

```bash
# Ver contenedores corriendo
docker-compose ps

# Debe mostrar los 7:
# afi_frontend_nova
# afi_backend
# afi_postgres
# afi_qdrant
# afi_ollama
# clone_full_backend
# clone_full_postgres
```

---

## 📊 Acceso en Servidor

### URLs
| Servicio | URL | Puerto |
|----------|-----|--------|
| Frontend | http://servidor:3000 | 3000 |
| Backend Principal | http://servidor:8000 | 8000 |
| Backend 926 | http://servidor:8011 | 8011 |
| Qdrant | http://servidor:6333 | 6333 |
| Ollama | http://servidor:11434 | 11434 |

### Con Nginx Reverse Proxy (Recomendado)

```nginx
server {
    listen 80;
    server_name tudominio.com;

    # Frontend
    location / {
        proxy_pass http://localhost:3000;
    }

    # Backend Principal
    location /api/ {
        proxy_pass http://localhost:8000/;
    }

    # Backend 926
    location /api-926/ {
        proxy_pass http://localhost:8011/;
    }

    # Qdrant
    location /qdrant/ {
        proxy_pass http://localhost:6333/;
    }
}
```

---

## 🔧 Comandos Útiles

```bash
# Ver logs de todos
docker-compose logs -f

# Ver logs de un servicio
docker-compose logs -f afi_backend

# Reiniciar un servicio
docker-compose restart afi_backend

# Detener todo
docker-compose down

# Limpiar volúmenes (CUIDADO: Borra datos)
docker-compose down -v

# Ver uso de recursos
docker stats

# Acceder a contenedor
docker exec -it afi_backend bash
```

---

## 💾 Backup de Bases de Datos

### Backup Automático Diario

```bash
#!/bin/bash
# archivo: backup.sh

BACKUP_DIR="/backups/jbafi"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Backup BD Principal
docker exec afi_postgres pg_dump -U afi_user -d afiliaciones > $BACKUP_DIR/afi_db_$TIMESTAMP.sql

# Backup BD 926
docker exec clone_full_postgres pg_dump -U admin -d migracion > $BACKUP_DIR/clone_db_$TIMESTAMP.sql

# Backup Qdrant
docker exec afi_qdrant tar czf /tmp/qdrant_$TIMESTAMP.tar.gz /qdrant/storage
cp /tmp/qdrant_$TIMESTAMP.tar.gz $BACKUP_DIR/

echo "Backup completado: $TIMESTAMP"
```

```bash
# Agregar al crontab
crontab -e

# Agregar línea:
0 2 * * * /home/user/backup.sh
```

---

## 🔒 Seguridad

### 1. Cambiar Contraseñas

Edita `.env`:
```bash
POSTGRES_PASSWORD=NUEVA_CONTRASEÑA_SEGURA
CLONE_DB_PASSWORD=NUEVA_CONTRASEÑA_SEGURA
```

### 2. Firewall

```bash
# Solo puertos necesarios
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable
```

### 3. SSL/TLS (Let's Encrypt)

```bash
# Con Certbot + Nginx
sudo apt install certbot python3-certbot-nginx
sudo certbot certonly --nginx -d tudominio.com
```

---

## 📈 Monitoreo

### Health Checks Configurados

Todos los 7 contenedores tienen healthchecks:

```bash
# Ver estado de salud
docker ps --format "table {{.Names}}\t{{.Status}}"

# Ejemplo de salida:
# afi_backend         Up 2 hours (healthy)
# afi_postgres        Up 2 hours (healthy)
```

### Logs Centralizados (Opcional)

```bash
# Con ELK Stack o Loki
docker-compose -f docker-compose-production.yml -f docker-compose-monitoring.yml up -d
```

---

## 🔄 Actualizar Código

```bash
# Pull de cambios
git pull origin main

# Rebuild de imágenes
docker-compose -f docker-compose-production.yml build --no-cache

# Restart de servicios
docker-compose -f docker-compose-production.yml up -d
```

---

## 📋 Checklist de Deployment

- [ ] Server preparado (Docker, Docker Compose, Git)
- [ ] Repositorio clonado
- [ ] `.env.production` configurado
- [ ] `docker-compose up -d` ejecutado
- [ ] Todos los 7 contenedores corriendo (`docker ps`)
- [ ] Frontend accesible (http://servidor:3000)
- [ ] Backend responde (http://servidor:8000/health)
- [ ] BD principal conectada
- [ ] BD 926 conectada
- [ ] Backups configurados
- [ ] Firewall configurado
- [ ] SSL/TLS instalado

---

## 🆘 Troubleshooting

### Contenedor no inicia
```bash
docker-compose logs SERVICE_NAME
```

### Puerto ya en uso
```bash
lsof -i :3000  # Encontrar proceso
kill -9 PID    # Matar proceso
```

### Memoria insuficiente
```bash
# Aumentar swap
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

## 📞 Soporte

- Logs: `docker-compose logs -f`
- Documentación: Ver README.md
- Issues: https://github.com/TU-USUARIO/jbafi/issues

---

**¡Sistema listo para producción!** 🎉
