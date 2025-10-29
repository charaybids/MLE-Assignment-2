# ML Pipeline - Loan Default Prediction System

[![Airflow](https://img.shields.io/badge/Airflow-3.1.0-017CEE?style=flat&logo=apache-airflow)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat&logo=apache-spark)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker)](https://www.docker.com/)

A production-ready machine learning pipeline for loan default prediction using Apache Airflow, PySpark, and Docker. Features automated training, inference, and monitoring with governance-based retraining.

---

## 📑 Table of Contents
- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Pipeline Execution](#-pipeline-execution)
- [Monitoring & Outputs](#-monitoring--outputs)
- [Troubleshooting](#-troubleshooting)
- [Project Structure](#-project-structure)
- [Technical Details](#-technical-details)

---

## 🚀 Features

- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **3-DAG Workflow**: Training, Inference, and Monitoring pipelines
- **Parallel Processing**: 4 parallel Bronze + 4 parallel Silver tasks (2x faster)
- **Automated Governance**: Auto-retraining when AUC < 0.70 or PSI ≥ 0.2
- **Multi-Model Training**: Logistic Regression, Random Forest, Gradient Boosting
- **Drift Detection**: Population Stability Index (PSI) calculation
- **Visual Monitoring**: 6-panel performance dashboard
- **Production Ready**: Docker Compose orchestration with health checks

---

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Compose                       │
├─────────────────────────────────────────────────────────┤
│  PostgreSQL → Airflow Init → Webserver + Scheduler      │
│                           ↓                              │
│                   DAG Processor (Airflow 3.x)            │
└─────────────────────────────────────────────────────────┘
```

### ML Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│  1️⃣ Training DAG (Weekly - Sunday 2 AM)                      │
│     Bronze (4∥) → Silver (4∥) → QC → Gold → Train → Store   │
│     Duration: ~72 minutes                                    │
├─────────────────────────────────────────────────────────────┤
│  2️⃣ Inference DAG (Daily - 3 AM)                             │
│     Prepare → Retrieve Model → Predict → Store              │
│     Duration: ~16 minutes                                    │
├─────────────────────────────────────────────────────────────┤
│  3️⃣ Monitoring DAG (Daily - 4 AM)                            │
│     Fetch → Calculate → Store → [Visualize, Governance]    │
│     Duration: ~8 minutes                                     │
│     Governance: AUC<0.70 OR PSI≥0.2 → Trigger Retraining   │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚡ Quick Start

### Prerequisites
- Docker Desktop (16GB RAM, 20GB disk space)
- Windows 10/11 with WSL2, macOS, or Linux

### 1. Start the Pipeline

```powershell
cd code
docker-compose up -d
```

Wait ~30 seconds for services to initialize.

### 2. Verify Services

```powershell
docker ps
```

All containers should show `(healthy)` status:
- `ml_pipeline_postgres`
- `ml_pipeline_webserver`
- `ml_pipeline_scheduler`
- `ml_pipeline_dag_processor`

### 3. Access Airflow UI

**URL**: http://localhost:8080  
**Username**: `admin`  
**Password**: `YxUpZHGypuNWERpF`

### 4. Trigger Training DAG

1. Login to Airflow UI
2. Find `ml_training_pipeline` DAG
3. Toggle switch **ON** (unpause)
4. Click **▶ Play** button → **"Trigger DAG"**
5. Click DAG name to open **Graph View**
6. Monitor progress (~72 minutes)

### 5. Run Inference & Monitoring

After training completes:

**Inference** (generates predictions):
- Unpause and trigger `ml_inference_pipeline` (~16 min)

**Monitoring** (performance dashboard + governance):
- Unpause and trigger `ml_monitoring_governance_pipeline` (~8 min)

---

## 📊 Pipeline Execution

### Training Pipeline (Weekly)

**Schedule**: Sunday 2:00 AM  
**Duration**: ~72 minutes

**Task Flow**:
```
Ingest Clickstream ─┐
Ingest Attributes  ─┼─► Quality Check ─► Gold Features
Ingest Financials  ─┤                      ↓
Ingest Loan Daily  ─┘                  ML Training
         ↓                                  ↓
Clean Clickstream  ─┐                  Store Model
Clean Attributes   ─┤
Clean Financials   ─┤
Clean Loan Daily   ─┘
```

**Outputs**:
- `datamart/bronze/` - 4 parquet files (raw ingestion)
- `datamart/silver/` - 4 parquet files (cleaned data)
- `datamart/gold/model_training_data.parquet` - 15 engineered features
- `model_store/best_model_*.pkl` - Best model (by AUC)
- `model_store/label_encoders_*.pkl` - Encoders
- `model_store/model_metadata_*.json` - Metrics (AUC, Precision, Recall, F1)

### Inference Pipeline (Daily)

**Schedule**: Daily 3:00 AM  
**Duration**: ~16 minutes

**Task Flow**:
```
Prepare Data → Retrieve Latest Model → Predict → Store Predictions
```

**Outputs**:
- `datamart/gold/model_predictions.parquet`
  - Columns: `customer_id`, `prediction`, `prediction_proba`, `actual_label`

### Monitoring Pipeline (Daily)

**Schedule**: Daily 4:00 AM  
**Duration**: ~8 minutes

**Task Flow**:
```
Fetch Predictions → Calculate Metrics → Store Metrics
                                              ↓
                    ┌─────────────────────────┴─────────────────────┐
                    ↓                                               ↓
              Visualize Report                            Governance Gate
                                                      (AUC < 0.70 OR PSI ≥ 0.2?)
                                                                    ↓
                                                    ┌───────────────┴────────────┐
                                                    ↓                            ↓
                                          Trigger Retraining              End (OK)
```

**Governance Thresholds**:
- **AUC-ROC** < 0.70 → Trigger retraining
- **PSI** ≥ 0.2 (Critical Drift) → Trigger retraining

**Outputs**:
- `datamart/gold/model_monitoring.parquet` - Metrics history
- `reports/performance_dashboard.png` - 6-panel dashboard:
  - ROC Curve
  - Precision-Recall Curve
  - Confusion Matrix
  - Prediction Distribution
  - Monthly AUC Trend
  - Monthly Metrics Comparison

---

## 🔍 Monitoring & Outputs

### Real-Time Monitoring

**Airflow UI - Task Status**:
- 🟢 **Green**: Success
- 🟡 **Yellow**: Running
- 🔴 **Red**: Failed
- ⚪ **White**: Queued

**View Logs**:
1. Click task in Graph View
2. Click **"Log"** button

### Docker Logs

```powershell
# Webserver logs
docker logs ml_pipeline_webserver --tail 100 -f

# Scheduler logs
docker logs ml_pipeline_scheduler --tail 100 -f

# DAG Processor logs
docker logs ml_pipeline_dag_processor --tail 100 -f
```

### Check Outputs

```powershell
# Model artifacts
ls model_store/

# Predictions
ls datamart/gold/

# Dashboard
ls reports/
```

---

## 🔧 Troubleshooting

### Containers Won't Start

**Symptoms**: `docker ps` shows no containers

**Solution**:
```powershell
docker-compose down -v
docker-compose up -d
docker-compose logs -f
```

### DAGs Not Appearing

**Symptoms**: Airflow UI shows 0 DAGs

**Diagnosis**:
```powershell
docker logs ml_pipeline_dag_processor --tail 100
docker exec ml_pipeline_scheduler airflow dags list-import-errors
```

**Solution**:
- Wait 30 seconds (DAG Processor needs time)
- Verify dag-processor container is running
- Check for Python import errors in logs

### Task Failures

**Common Causes**:
- Missing input files in `data/` folder
- Insufficient Docker memory (need 12GB+)
- Python package issues

**Solution**:
```powershell
# Verify input files exist
ls data/
# Should have: feature_clickstream.csv, features_attributes.csv, 
#              features_financials.csv, lms_loan_daily.csv

# Increase Docker memory (Docker Desktop → Settings → Resources)
# Recommended: 12GB+

# Retry failed task in Airflow UI (click "Clear" on task)
```

### Port 8080 Already in Use

**Solution**:
```powershell
# Find process using port 8080
netstat -ano | Select-String "8080"

# Kill process (replace PID)
Stop-Process -Id <PID> -Force

# OR change port in docker-compose.yaml
# "8080:8080" → "8081:8080"
```

---

## 📁 Project Structure

```
Assignment_2/
├── code/
│   ├── dags/                          # Airflow DAG definitions
│   │   ├── ml_training_dag.py         # Training pipeline (11 tasks)
│   │   ├── ml_inference_dag.py        # Inference pipeline (4 tasks)
│   │   └── ml_monitoring_dag.py       # Monitoring + governance (6 tasks)
│   │
│   ├── scripts/                       # Core pipeline logic
│   │   ├── bronze_pipeline.py         # CSV → Parquet ingestion
│   │   ├── silver_pipeline.py         # PySpark data cleaning
│   │   ├── gold_pipeline.py           # Feature engineering (15 features)
│   │   ├── ml_training.py             # Train 3 models, select best
│   │   ├── ml_inference.py            # Generate predictions
│   │   ├── ml_monitoring.py           # PSI calculation, metrics
│   │   └── generate_report.py         # Dashboard visualization
│   │
│   ├── docker-compose.yaml            # 5-service orchestration
│   ├── Dockerfile                     # Python 3.11 + PySpark image
│   ├── requirements.txt               # Python dependencies
│   └── ARCHITECTURE.md                # Detailed technical spec
│
├── data/                              # Input CSV files
│   ├── feature_clickstream.csv
│   ├── features_attributes.csv
│   ├── features_financials.csv
│   └── lms_loan_daily.csv
│
├── datamart/                          # Processed data (Medallion)
│   ├── bronze/                        # Raw parquet files
│   ├── silver/                        # Cleaned parquet files
│   └── gold/                          # Feature-engineered data
│
├── model_store/                       # ML artifacts
│   ├── best_model_*.pkl
│   ├── label_encoders_*.pkl
│   └── model_metadata_*.json
│
└── reports/                           # Monitoring dashboards
    └── performance_dashboard.png
```

---

## 🔧 Technical Details

### Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 3.1.0 | Workflow orchestration |
| Python | 3.11 | Runtime environment |
| PySpark | 3.5.0 | Distributed data processing |
| PostgreSQL | 16-alpine | Airflow metadata store |
| Docker Compose | Latest | Container orchestration |
| Scikit-learn | 1.3+ | ML model training |
| Matplotlib | 3.7+ | Dashboard visualization |

### Airflow Configuration

- **Executor**: LocalExecutor
- **Parallelism**: 16 (max concurrent tasks)
- **DAG Concurrency**: 8 (per DAG)
- **Max Active Runs**: 1 (per DAG)
- **Auth**: Simple Auth Manager (auto-generated password)

### Data Processing

**Bronze Layer**:
- 4 CSV files → Parquet with gzip compression
- Fallback: pyarrow → fastparquet

**Silver Layer** (PySpark):
- Date parsing: 5 columns (d/M/yyyy format)
- Attributes cleaning: Occupation, Age regex, SSN validation
- Financials cleaning: 3 categorical + 9 float + 6 int columns
- Loan daily cleaning: 7 integer columns
- Clickstream cleaning: 20 features (signed integer support)
- Customer filtering: Remove flagged customers across all datasets

**Gold Layer**:
- 15 engineered features:
  - `Credit_History_Months`, `DTI`, `Savings_Ratio`
  - `Monthly_Surplus`, `Debt_to_Annual_Income`
  - `hist_total_paid`, `hist_Loan_Payment_Ratio`
  - `fe_10_mean`, `fe_10_std`, etc.
- Time-aware filtering (clickstream before loan date)
- Median imputation for 15 columns + Age

### ML Models

**Training**:
- Logistic Regression (max_iter=1000)
- Random Forest (n_estimators=100, max_depth=10)
- Gradient Boosting (n_estimators=100, max_depth=5)

**Selection Criteria**: Best AUC-ROC on test set

**Artifacts**:
- Model pickle file
- Label encoders (for categorical features)
- Metadata JSON (AUC, Precision, Recall, F1, timestamp)

### Monitoring Metrics

**Performance**:
- AUC-ROC (threshold: 0.70)
- Precision, Recall, F1-Score

**Drift Detection**:
- PSI calculation (10 bins)
- Thresholds:
  - PSI < 0.1: No Drift
  - 0.1 ≤ PSI < 0.2: Warning
  - PSI ≥ 0.2: Critical Drift (trigger retraining)

---

## 🛠️ Maintenance

### Regular Operations

```powershell
# Check disk space
Get-ChildItem -Path datamart -Recurse | Measure-Object -Property Length -Sum

# Archive old models (keep last 5)
ls model_store/ | Sort-Object LastWriteTime -Descending | Select-Object -Skip 5 | Remove-Item

# Archive old reports (keep last 10)
ls reports/ | Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 | Remove-Item
```

### Stop & Restart

```powershell
# Stop containers (keep data)
docker-compose stop

# Stop and remove containers (keep volumes)
docker-compose down

# Full cleanup (DELETE ALL DATA)
docker-compose down -v

# Restart
docker-compose up -d
```

### Update Pipeline Code

```powershell
# After code changes
docker-compose down
docker-compose build
docker-compose up -d

# DAG files only (no restart needed)
# DAG Processor auto-detects changes in ~30 seconds
```

---

## 🔐 Security Notes

**Default Credentials**:
- **Airflow**: `admin` / `YxUpZHGypuNWERpF` (auto-generated)
- **PostgreSQL**: `airflow` / `airflow` (Docker network only)
- **Fernet Key**: `PAqBeGJLJTYFzVkOGHWIYXdLO7XdXz5yTdxAGJe9ezM=`

**Best Practices**:
- Change Airflow password after first login
- Use `.env` file for credentials (not git)
- Rotate Fernet key periodically
- Enable SSL for production

---

## 📈 Performance Metrics

### Expected Timeline

| Pipeline | Duration | Frequency |
|----------|----------|-----------|
| Training | ~72 min | Weekly (Sunday 2 AM) |
| Inference | ~16 min | Daily (3 AM) |
| Monitoring | ~8 min | Daily (4 AM) |

### Parallelization Benefits

- **Bronze**: 4 parallel tasks (4 min → 2 min, 2x faster)
- **Silver**: 4 parallel tasks (30 min → 15 min, 2x faster)
- **Total Training**: 3 hours → 1.5 hours

### Resource Usage

- **Memory**: ~12GB peak (PySpark training)
- **CPU**: 4-8 cores during parallel tasks
- **Disk**: ~2GB for datamart artifacts
- **Network**: Minimal (local Docker)

---

## ✅ Deployment Checklist

- [ ] Docker Desktop installed and running
- [ ] Input CSV files in `data/` folder (4 files)
- [ ] `docker-compose up -d` successful
- [ ] All 5 containers healthy
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] All 3 DAGs visible
- [ ] Training DAG completed (green status)
- [ ] Model artifacts in `model_store/`
- [ ] Predictions in `datamart/gold/`
- [ ] Dashboard in `reports/`

---

## 📞 Support

### Useful Commands

```powershell
# List all DAGs
docker exec ml_pipeline_webserver airflow dags list

# Trigger DAG from CLI
docker exec ml_pipeline_scheduler airflow dags trigger ml_training_pipeline

# Check DAG runs
docker exec ml_pipeline_webserver airflow dags list-runs -d ml_training_pipeline

# Access PostgreSQL
docker exec -it ml_pipeline_postgres psql -U airflow

# Check Airflow version
docker exec ml_pipeline_webserver airflow version
```

### Quick Links

- **Airflow UI**: http://localhost:8080
- **PostgreSQL**: `localhost:5432` (Docker network only)
- **Detailed Architecture**: See `code/ARCHITECTURE.md`

---

## 📝 License

This project is for educational purposes as part of CS611 - Machine Learning Engineering coursework.

---

**Version**: 1.0  
**Last Updated**: October 27, 2025  
**Airflow**: 3.1.0  
**Status**: Production Ready ✅
