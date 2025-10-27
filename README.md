# ML Pipeline with Apache Airflow

A production-grade machine learning pipeline for loan default prediction, orchestrated with Apache Airflow 2.10.3 and containerized with Docker.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose
- 8GB RAM minimum (16GB recommended)
- Port 8080 available

### Start the Pipeline

```powershell
# Navigate to code directory
cd code

# Build and start services
docker-compose up -d

# Wait ~30 seconds for services to initialize
# Access Airflow UI at http://localhost:8080
# Login: admin / admin
```

### Run the Complete Pipeline

1. **Training Pipeline** (creates the model)
   - Unpause `ml_training_pipeline` in Airflow UI
   - Click "Trigger DAG" button
   - Wait ~1-2 minutes for completion
   - Best model saved to `model_store/`

2. **Inference Pipeline** (generates predictions)
   - Trigger `ml_inference_pipeline`
   - Wait ~5-10 seconds
   - Predictions saved to `datamart/gold/model_predictions.parquet`

3. **Monitoring Pipeline** (evaluates performance)
   - Trigger `ml_monitoring_governance_pipeline`
   - Wait ~30-60 seconds
   - Dashboard saved to `reports/performance_dashboard.png`

---

## 📊 Architecture

### Medallion Data Lake Pattern

```
CSV Files (data/)
    ↓
[Bronze Layer] - Raw ingestion (4 parallel tasks)
    ↓
[Silver Layer] - Data cleaning (4 parallel tasks)
    ↓
[Quality Check] - Remove flagged customers
    ↓
[Gold Layer] - Feature engineering (15 features)
    ↓
[ML Training] - Train 3 models, select best
    ↓
[Model Store] - Persist best model + metadata
    ↓
[Inference] - Generate predictions
    ↓
[Monitoring] - Calculate metrics + dashboard
    ↓
[Governance] - Trigger retraining if needed
```

### Tech Stack

- **Orchestration:** Apache Airflow 2.10.3 (LocalExecutor)
- **Database:** PostgreSQL 16-alpine
- **Processing:** PySpark 3.5.0 + Pandas 2.2.0
- **ML Framework:** Scikit-learn 1.4.0
- **Containerization:** Docker + Docker Compose
- **Storage:** Parquet files (Bronze/Silver/Gold layers)

---

## 📁 Project Structure

```
code/
├── dags/                           # Airflow DAG definitions
│   ├── ml_training_dag.py         # Training pipeline (11 tasks)
│   ├── ml_inference_dag.py        # Inference pipeline (4 tasks)
│   └── ml_monitoring_dag.py       # Monitoring pipeline (6 tasks)
│
├── scripts/                        # Pipeline execution scripts
│   ├── bronze_pipeline.py         # Raw data ingestion
│   ├── silver_pipeline.py         # Data cleaning
│   ├── gold_pipeline.py           # Feature engineering
│   ├── ml_training.py             # Model training
│   ├── ml_inference.py            # Prediction generation
│   ├── ml_monitoring.py           # Performance monitoring
│   └── generate_report.py         # Dashboard creation
│
├── data/                           # Input CSV files (34 MB)
│   ├── feature_clickstream.csv
│   ├── features_attributes.csv
│   ├── features_financials.csv
│   └── lms_loan_daily.csv
│
├── datamart/                       # Processed data layers
│   ├── bronze/                    # Raw parquet files
│   ├── silver/                    # Cleaned data
│   └── gold/                      # Features + predictions
│
├── model_store/                    # ML artifacts
│   ├── best_model_*.pkl           # Selected model
│   ├── label_encoders_*.pkl       # Categorical encoders
│   └── model_metadata_*.json      # Model info + metrics
│
├── reports/                        # Visualizations
│   └── performance_dashboard.png  # 6-chart dashboard
│
├── logs/                           # Airflow execution logs
│   └── dag_id=*/run_id=*/...      # Task logs by DAG run
│
├── docker-compose.yaml             # Service orchestration
├── Dockerfile                      # Custom Airflow image
├── requirements.txt                # Python dependencies
├── ARCHITECTURE.md                 # Detailed architecture
└── TROUBLESHOOTING.md             # Common issues + fixes
```

---

## 🎯 DAG Details

### 1. Training Pipeline (`ml_training_pipeline`)

**Purpose:** Ingest data, clean, engineer features, train models, select best

**Tasks (11 total):**
```
ingest_clickstream  ──┐
ingest_attributes   ──┼──> clean_clickstream  ──┐
ingest_financials   ──┤    clean_attributes   ──┼──> quality_check
ingest_loan_daily   ──┘    clean_financials   ──┤         ↓
                           clean_loan_daily   ──┘   gold_pipeline
                                                           ↓
                                                   train_evaluate_select
                                                           ↓
                                                    store_best_model
```

**Schedule:** Daily at 2 AM (`0 2 * * *`)

**Models Trained:**
1. Logistic Regression (baseline)
2. Random Forest (ensemble)
3. Gradient Boosting (ensemble)

**Selection Criteria:** Highest Test AUC

**Duration:** ~1-2 minutes

**Outputs:**
- `model_store/best_model_YYYYMMDD_HHMMSS.pkl`
- `model_store/label_encoders_YYYYMMDD_HHMMSS.pkl`
- `model_store/model_metadata_YYYYMMDD_HHMMSS.json`

---

### 2. Inference Pipeline (`ml_inference_pipeline`)

**Purpose:** Load latest model, generate predictions on new data

**Tasks (4 total):**
```
prepare_inference_data → retrieve_best_model → make_predictions → store_predictions_gold
```

**Schedule:** Daily at 6 AM (`0 6 * * *`)

**Duration:** ~5-10 seconds

**Outputs:**
- `datamart/gold/model_predictions.parquet` (columns: Customer_ID, loan_id, prediction, prediction_proba, actual_label)

---

### 3. Monitoring Pipeline (`ml_monitoring_governance_pipeline`)

**Purpose:** Calculate performance metrics, generate dashboard, trigger retraining if needed

**Tasks (6 total):**
```
fetch_gold_data → calculate_monitoring_metrics → store_monitoring_results → visualize_dashboard
                                                                                    ↓
                                                                          governance_gate_check
                                                                                    ↓
                                                          ┌─────────────────────────┴──────────────────┐
                                                    [Model OK]                              [Model Degraded]
                                                          ↓                                              ↓
                                                  end_monitoring_ok                         trigger_retraining_dag
```

**Schedule:** Daily at 8 AM (`0 8 * * *`)

**Metrics Calculated:**
- AUC (Area Under ROC Curve)
- Precision, Recall, F1 Score
- PSI (Population Stability Index)

**Retraining Triggers:**
- AUC < 0.70 **OR**
- PSI ≥ 0.2 **OR**
- Precision < 0.60 **OR**
- Recall < 0.50

**Duration:** ~30-60 seconds

**Outputs:**
- `datamart/gold/model_monitoring.parquet` (metrics by month)
- `reports/performance_dashboard.png` (6 visualizations)

---

## 📈 Dashboard Visualizations

The performance dashboard (`reports/performance_dashboard.png`) contains:

1. **ROC Curve** - True Positive Rate vs False Positive Rate
2. **Precision-Recall Curve** - Trade-off between precision and recall
3. **Confusion Matrix** - Classification results breakdown
4. **Prediction Distribution** - Histogram of prediction probabilities
5. **Monthly AUC Trend** - Model performance over time
6. **Monthly Metrics** - Precision, Recall, F1 trends

---

## 🛠️ Configuration

### Environment Variables (docker-compose.yaml)

```yaml
# Executor settings
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: 16
AIRFLOW__CORE__DAG_CONCURRENCY: 8
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1

# PySpark settings
SPARK_DRIVER_MEMORY: 16g
PYSPARK_PYTHON: python3
PYSPARK_DRIVER_PYTHON: python3

# Security
AIRFLOW__CORE__FERNET_KEY: PAqBeGJLJTYFzVkOGHWIYXdLO7XdXz5yTdxAGJe9ezM=
AIRFLOW__WEBSERVER__SECRET_KEY: airflow_secret_key_2024
```

### Volume Mounts

All data persists on host machine:

```yaml
volumes:
  - ./dags:/opt/airflow/dags              # DAG files
  - ./scripts:/opt/airflow/scripts        # Pipeline scripts
  - ./data:/opt/airflow/data              # Input CSVs
  - ./datamart:/opt/airflow/datamart      # Processed data
  - ./model_store:/opt/airflow/model_store # ML models
  - ./reports:/opt/airflow/reports        # Dashboards
  - ./logs:/opt/airflow/logs              # Execution logs
```

---

## 🔧 Management Commands

### Check Container Status
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### View Container Logs
```powershell
docker logs ml_pipeline_webserver
docker logs ml_pipeline_scheduler
```

### Access Airflow CLI
```bash
# List all DAGs
docker exec ml_pipeline_scheduler airflow dags list

# Trigger a DAG manually
docker exec ml_pipeline_scheduler airflow dags trigger ml_training_pipeline

# Check DAG run status
docker exec ml_pipeline_scheduler airflow dags list-runs -d ml_training_pipeline
```

### Restart Services
```powershell
docker-compose restart airflow-webserver airflow-scheduler
```

### Stop Services
```powershell
docker-compose down
```

### Complete Reset (Delete All Data)
```powershell
# Stop and remove containers + database
docker-compose down -v

# Clean generated files
Remove-Item -Recurse -Force ./logs/*
Remove-Item -Recurse -Force ./datamart/bronze/*
Remove-Item -Recurse -Force ./datamart/silver/*
Remove-Item -Recurse -Force ./datamart/gold/*
Remove-Item -Recurse -Force ./model_store/*
Remove-Item -Recurse -Force ./reports/*

# Start fresh
docker-compose up -d
```

---

## 📊 Expected Results

### Training Pipeline Output

**Model Comparison (typical results):**

| Model | Train AUC | Test AUC | Selected |
|-------|-----------|----------|----------|
| Logistic Regression | 0.7289 | 0.7241 | ❌ |
| Random Forest | 0.9227 | 0.7895 | ❌ |
| **Gradient Boosting** | 0.9041 | **0.7898** | ✅ |

**Features Used (15 total):**
- `Credit_History_Months`, `Age`, `Monthly_Inhand_Salary`
- `loan_amt`, `tenure`, `Interest_Rate`
- `fe_10_mean`, `fe_10_std` (clickstream aggregates)
- `Savings_Ratio`, `DTI` (derived features)
- `Num_Bank_Accounts`, `Num_Credit_Card`, `Amount_invested_monthly`
- `Credit_Mix`, `Occupation` (categorical)

### File Sizes

| Layer | Size |
|-------|------|
| Input CSVs | ~34 MB |
| Bronze Parquet | ~20 MB |
| Silver Parquet | ~15 MB |
| Gold Parquet | ~8 MB |
| Model Files | ~500 KB |
| Dashboard PNG | ~687 KB |

---

## 🐛 Troubleshooting

### Common Issues

**Problem:** Tasks stuck in "queued" state
- **Solution:** Check scheduler logs, verify LocalExecutor configured correctly

**Problem:** "FileNotFoundError" for CSV files
- **Solution:** Ensure all CSV files are in `code/data/` directory

**Problem:** Feature column mismatch errors
- **Solution:** Ensure training and inference use same feature list

**Problem:** Dashboard not generated
- **Solution:** Check task dependencies in monitoring DAG

**For detailed troubleshooting, see [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)**

---

## 📚 Additional Documentation

- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Detailed system design, data flow, and component descriptions
- **[TROUBLESHOOTING.md](./TROUBLESHOOTING.md)** - Complete issue resolution guide with 9 documented problems and solutions

---

## 🎓 Key Features

✅ **Medallion Architecture** - Bronze/Silver/Gold data layers  
✅ **Parallel Processing** - Concurrent task execution for speed  
✅ **Model Selection** - Automatic best model selection by AUC  
✅ **Automated Monitoring** - Performance tracking with PSI  
✅ **Governance Gates** - Automatic retraining triggers  
✅ **Containerized** - Fully portable Docker deployment  
✅ **Persistent Storage** - All artifacts saved on host machine  
✅ **Comprehensive Logging** - Debug with task-level logs  

---

## 🔐 Security Notes

- Default credentials: `admin/admin` (change in production!)
- Fernet key provided for encryption (generate new for production)
- No external network exposure by default
- All data stays local (no cloud dependencies)

---

## 📝 License

This project is for educational purposes (CS611 - MLE Assignment 2).

---

## 👥 Support

For issues or questions:
1. Check [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) first
2. Review Airflow UI logs (Logs tab)
3. Inspect `code/logs/` directory on host
4. Check container logs with `docker logs`

---

## 🚦 System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disk | 10 GB free | 20 GB free |
| Docker | 20.10+ | Latest |

---

## ⚡ Performance Tips

1. **Increase parallelism** for faster processing (edit `AIRFLOW__CORE__PARALLELISM`)
2. **Allocate more memory** to Spark driver (edit `SPARK_DRIVER_MEMORY`)
3. **Use SSD storage** for better I/O performance
4. **Monitor container resources** with `docker stats`

---

## 🎯 Next Steps

After successful deployment:

1. ✅ Verify all 3 DAGs complete successfully
2. ✅ Review generated dashboard in `reports/`
3. ✅ Inspect model metadata in `model_store/`
4. ✅ Check logs for training output (model selection details)
5. ✅ Set up scheduled execution (DAGs run automatically)
6. ✅ Customize retraining thresholds in monitoring DAG
7. ✅ Add custom features or models as needed

---

**Version:** Airflow 2.10.3 | Python 3.11 | PySpark 3.5.0  
**Last Updated:** October 28, 2025
