# Deployment Guide

This guide helps you deploy the ML Pipeline on a new environment.

## 🚀 Quick Deploy

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine + Docker Compose (Linux)
- Git
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### Step 1: Clone the Repository

```bash
git clone https://github.com/charaybids/MLE-Assignment-2.git
cd MLE-Assignment-2
```

### Step 2: Start Services

```bash
# Build and start all services
docker-compose up -d

# Wait ~30 seconds for initialization
sleep 30

# Check status
docker ps
```

You should see 3 containers running:
- `ml_pipeline_postgres` (healthy)
- `ml_pipeline_webserver` (healthy)
- `ml_pipeline_scheduler` (healthy)

### Step 3: Access Airflow UI

Open browser: http://localhost:8080

**Login Credentials:**
- Username: `admin`
- Password: `admin`

### Step 4: Run the Pipeline

1. **Training Pipeline** (run first)
   - Unpause `ml_training_pipeline`
   - Click "Trigger DAG" ▶️
   - Duration: ~1-2 minutes
   - Trains 3 models, selects best

2. **Inference Pipeline** (run after training)
   - Trigger `ml_inference_pipeline`
   - Duration: ~5-10 seconds
   - Generates predictions

3. **Monitoring Pipeline** (run after inference)
   - Trigger `ml_monitoring_governance_pipeline`
   - Duration: ~30-60 seconds
   - Creates dashboard

### Step 5: View Results

```bash
# Performance dashboard (687KB PNG with 6 charts)
ls -lh reports/performance_dashboard.png

# Model files
ls -lh model_store/

# Predictions
ls -lh datamart/gold/model_predictions.parquet
```

---

## 🌐 Cloud Deployment Options

### Option 1: AWS EC2

```bash
# Launch EC2 instance (t3.xlarge or larger)
# Amazon Linux 2 or Ubuntu 22.04

# Install Docker
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Clone and deploy
git clone https://github.com/charaybids/MLE-Assignment-2.git
cd MLE-Assignment-2
docker-compose up -d

# Open port 8080 in Security Group
# Access: http://YOUR_EC2_IP:8080
```

### Option 2: Azure VM

```bash
# Create Ubuntu 22.04 VM (Standard_D4s_v3 or larger)

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose (if not included)
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Clone and deploy
git clone https://github.com/charaybids/MLE-Assignment-2.git
cd MLE-Assignment-2
docker-compose up -d

# Configure NSG to allow port 8080
# Access: http://YOUR_VM_IP:8080
```

### Option 3: Google Cloud Compute Engine

```bash
# Create VM (e2-standard-4 or larger, Ubuntu 22.04)

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Clone and deploy
git clone https://github.com/charaybids/MLE-Assignment-2.git
cd MLE-Assignment-2
docker-compose up -d

# Configure firewall rule for port 8080
# Access: http://YOUR_VM_IP:8080
```

### Option 4: Local Windows/Mac

```powershell
# Already covered in Quick Deploy above
# Use Docker Desktop
```

---

## 🔧 Configuration Changes for Production

### 1. Change Default Credentials

Edit `docker-compose.yaml`:

```yaml
environment:
  _AIRFLOW_WWW_USER_USERNAME: ${AIRFLOW_USERNAME:-your_username}
  _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_PASSWORD:-your_secure_password}
```

Or create `.env` file:
```env
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your_secure_password_here
```

### 2. Generate New Fernet Key

```bash
# Generate new key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Update docker-compose.yaml
AIRFLOW__CORE__FERNET_KEY: your_generated_key_here
```

### 3. Adjust Resources

Edit `docker-compose.yaml`:

```yaml
environment:
  # Increase parallelism for more powerful machines
  AIRFLOW__CORE__PARALLELISM: 32  # Default: 16
  AIRFLOW__CORE__DAG_CONCURRENCY: 16  # Default: 8
  
  # Increase Spark memory for larger datasets
  SPARK_DRIVER_MEMORY: 32g  # Default: 16g
```

### 4. Enable HTTPS (Production)

Use a reverse proxy (Nginx or Traefik):

```nginx
# nginx.conf
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## 🗄️ Persistent Storage Configuration

### Mount External Volume (Recommended for Cloud)

```yaml
# docker-compose.yaml
volumes:
  postgres_data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /mnt/data/postgres  # Your persistent disk mount
```

### Backup Important Data

```bash
# Backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)

# Backup model store
tar -czf model_store_backup_${DATE}.tar.gz model_store/

# Backup datamart
tar -czf datamart_backup_${DATE}.tar.gz datamart/

# Backup PostgreSQL
docker exec ml_pipeline_postgres pg_dump -U airflow airflow > airflow_db_backup_${DATE}.sql
```

---

## 📊 Monitoring & Logs

### View Container Logs

```bash
# All logs
docker-compose logs -f

# Specific service
docker logs -f ml_pipeline_webserver
docker logs -f ml_pipeline_scheduler

# Task logs (also available in Airflow UI)
ls logs/dag_id=*/run_id=*/task_id=*/
```

### Health Checks

```bash
# Check container health
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check Airflow health
curl http://localhost:8080/health

# Check database connection
docker exec ml_pipeline_postgres psql -U airflow -c "SELECT version();"
```

---

## 🔄 Updates & Maintenance

### Pull Latest Changes

```bash
git pull origin main
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Restart Services

```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver
```

### Clean Start (Remove Old Data)

```bash
# Stop and remove everything including volumes
docker-compose down -v

# Clean generated files
rm -rf logs/* datamart/bronze/* datamart/silver/* datamart/gold/* model_store/* reports/*

# Start fresh
docker-compose up -d
```

---

## 🐛 Troubleshooting

### Containers Not Starting

```bash
# Check logs
docker-compose logs

# Rebuild images
docker-compose build --no-cache
docker-compose up -d
```

### Port 8080 Already in Use

```bash
# Find process using port
# Windows
netstat -ano | findstr :8080

# Linux/Mac
lsof -i :8080

# Change port in docker-compose.yaml
ports:
  - "8081:8080"  # Use 8081 instead
```

### Out of Memory Errors

```bash
# Increase Docker memory limit
# Docker Desktop -> Settings -> Resources -> Memory: 16GB

# Or reduce Spark memory in docker-compose.yaml
SPARK_DRIVER_MEMORY: 8g
```

### Tasks Stuck in Queued

```bash
# Check scheduler logs
docker logs ml_pipeline_scheduler

# Restart scheduler
docker-compose restart airflow-scheduler
```

For more issues, see [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)

---

## 📋 Post-Deployment Checklist

- [ ] All 3 containers healthy
- [ ] Airflow UI accessible at http://localhost:8080
- [ ] Can login with admin credentials
- [ ] All 3 DAGs visible in UI
- [ ] Training pipeline completed successfully
- [ ] Model files created in `model_store/`
- [ ] Inference pipeline completed
- [ ] Predictions created in `datamart/gold/`
- [ ] Monitoring pipeline completed
- [ ] Dashboard created in `reports/`
- [ ] Logs accessible in `logs/` directory

---

## 🔐 Security Recommendations

1. **Change default passwords** immediately
2. **Generate new Fernet key** for encryption
3. **Use HTTPS** in production (reverse proxy)
4. **Restrict port access** (firewall rules)
5. **Enable authentication** for API endpoints
6. **Regular backups** of database and models
7. **Update dependencies** regularly for security patches
8. **Use secrets management** (AWS Secrets Manager, Azure Key Vault)

---

## 📞 Support

- **Documentation**: See `README.md` for detailed guide
- **Architecture**: See `ARCHITECTURE.md` for system design
- **Troubleshooting**: See `TROUBLESHOOTING.md` for common issues
- **Repository**: https://github.com/charaybids/MLE-Assignment-2
- **Issues**: Report bugs on GitHub Issues

---

**Version:** Airflow 2.10.3 | Python 3.11 | PySpark 3.5.0  
**Last Updated:** October 28, 2025
