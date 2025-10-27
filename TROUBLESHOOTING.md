# Troubleshooting Guide - ML Pipeline with Airflow

## Overview
This document records all issues encountered during the deployment of the ML pipeline with Apache Airflow 2.10.3, including root causes and solutions.

---

## Issue 1: Airflow 3.1.0 Compatibility Problems

### Problem
Initially attempted to use Airflow 3.1.0, but encountered multiple breaking changes:

**Symptoms:**
- Tasks stayed in "queued" state forever and never executed
- Errors: `httpx.ConnectError: [Errno 111] Connection refused` when trying to reach `http://localhost:9091`
- DAGs not showing in UI despite being in `dags/` folder

**Root Cause:**
- Airflow 3.1.0 removed the `webserver` command (only `api-server` exists)
- New SDK-based task execution requires HTTP API communication on port 9091
- LocalExecutor workers unable to connect to internal supervisor API in containerized environment
- Python API changes: `days_ago` removed, `schedule_interval` renamed to `schedule`
- Requires separate `dag-processor` service for DAG parsing

**Solution:**
Downgraded to **Airflow 2.10.3** for stability:
```dockerfile
FROM apache/airflow:2.10.3-python3.11  # Changed from 3.1.0
```

---

## Issue 2: Conflicting Airflow Versions in requirements.txt

### Problem
Even after changing the Dockerfile base image to Airflow 2.10.3, containers were still running Airflow 3.1.0.

**Symptoms:**
```bash
$ docker exec ml_pipeline_webserver pip show apache-airflow
Version: 3.1.0  # Wrong version!
```

**Root Cause:**
`requirements.txt` had hardcoded `apache-airflow==3.1.0`, which overwrote the base image's 2.10.3 installation during the build process.

**Solution:**
Removed `apache-airflow==3.1.0` from `requirements.txt`:
```diff
- apache-airflow==3.1.0
  pandas==2.2.0
  pyspark==3.5.0
```

**Lesson Learned:**
Don't specify Airflow version in requirements.txt when using official Airflow Docker images - let the base image provide it.

---

## Issue 3: Webserver "airflow: command not found"

### Problem
After fixing the version conflict, webserver container failed to start with:
```
/entrypoint: line 20: airflow: command not found
ERROR! Maximum number of retries (20) reached.
```

**Root Cause:**
The `pip install` from requirements.txt upgraded Airflow to 3.1.0, breaking the PATH configuration that the base image expected.

**Solution:**
1. Removed all custom images: `docker rmi code-airflow-* -f`
2. Pulled fresh base image: `docker pull apache/airflow:2.10.3-python3.11`
3. Rebuilt with `--no-cache`: `docker-compose build --no-cache`
4. Fresh start: `docker-compose down -v && docker-compose up -d`

---

## Issue 4: Missing Data Files

### Problem
Tasks failed with `FileNotFoundError` for CSV input files:
```
FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/data/features_financials.csv'
```

**Root Cause:**
CSV files were in parent directory (`../data/`) but docker-compose was mounting `./data/` (which was empty).

**Solution:**
Copied all CSV files into the `code/data/` directory:
```powershell
Copy-Item "../data/*.csv" -Destination "./data/"
```

**Files moved:**
- `feature_clickstream.csv` (20 MB)
- `features_attributes.csv` (689 KB)
- `features_financials.csv` (2.9 MB)
- `lms_loan_daily.csv` (11 MB)

**Benefit:** Makes the `code/` folder completely self-contained and portable.

---

## Issue 5: Missing Datamart Subdirectories

### Problem
Silver layer tasks failed with:
```
OSError: Cannot save file into a non-existent directory: '/opt/***/datamart/silver'
```

**Root Cause:**
Only `datamart/bronze/` directory existed in `code/` folder. The `silver/` and `gold/` directories were in the parent folder.

**Solution:**
Copied directory structure:
```powershell
Copy-Item -Recurse "../datamart/silver" -Destination "./datamart/"
Copy-Item -Recurse "../datamart/gold" -Destination "./datamart/"
```

---

## Issue 6: Wrong Feature Columns in Inference Script

### Problem
Inference DAG failed with:
```
KeyError: "None of [Index(['age', 'fa_1', 'fa_2', ...)] are in the [columns]"
```

**Root Cause:**
`ml_inference.py` had hardcoded feature names that didn't match what `ml_training.py` actually used:

**Wrong (inference):**
```python
feature_cols = ['age'] + [f'fa_{i}' for i in range(1, 11)] + ['avg_dpd_obs', ...]
```

**Correct (training):**
```python
feature_cols = ['Credit_History_Months', 'Age', 'Monthly_Inhand_Salary', 'loan_amt', ...]
```

**Solution:**
Updated `ml_inference.py` to use the exact same feature columns as training:
```python
feature_cols = ['Credit_History_Months', 'Age', 'Monthly_Inhand_Salary', 'loan_amt', 'tenure',
                'Interest_Rate', 'fe_10_mean', 'fe_10_std', 'Savings_Ratio', 'DTI',
                'Num_Bank_Accounts', 'Num_Credit_Card', 'Amount_invested_monthly']
categorical_cols = ['Credit_Mix', 'Occupation']
```

Also added label encoder loading to handle categorical features properly.

---

## Issue 7: Hardcoded Paths in generate_report.py

### Problem
The report generation script used `Path(__file__).parent.parent.parent`, which resolved incorrectly in the Docker container:
```python
project_root = Path(__file__).parent.parent.parent  # Goes to /opt/ instead of /opt/airflow/
```

**Root Cause:**
- `__file__` = `/opt/airflow/scripts/generate_report.py`
- `.parent.parent.parent` = `/opt/` ❌ (one level too high)

**Solution:**
Changed to accept parameters instead of calculating path:
```python
def generate_report(gold_path='/opt/airflow/datamart/gold', 
                   reports_path='/opt/airflow/reports'):
    # Load data from provided paths
    predictions_df = pd.read_parquet(os.path.join(gold_path, "model_predictions.parquet"))
```

Updated DAG to pass correct paths:
```python
visualize_task = PythonOperator(
    task_id='visualize_monitoring_dashboard',
    python_callable=generate_report,
    op_kwargs={
        'gold_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
        'reports_path': os.path.join(PROJECT_ROOT, 'reports'),
    },
)
```

---

## Issue 8: Dashboard Not Generated (Skipped by Branch)

### Problem
The `visualize_monitoring_dashboard` task was skipped (shown in pink) and no dashboard PNG was created.

**Root Cause:**
Incorrect task dependency in `ml_monitoring_dag.py`:
```python
# WRONG - visualization downstream of governance gate
governance_gate >> visualize_task
governance_gate >> [trigger_retraining, end_ok_task]
```

When model performance was good, the branch went to `end_ok_task`, skipping the visualization.

**Solution:**
Moved visualization BEFORE the governance gate:
```python
# CORRECT - visualization always runs
fetch_task >> monitoring_task >> store_results_task >> visualize_task >> governance_gate
governance_gate >> [trigger_retraining, end_ok_task]
```

Now the dashboard is generated regardless of the governance decision.

---

## Issue 9: Logs Not Accessible on Host Machine

### Problem
Logs only existed inside Docker containers, not visible on the host machine for easy troubleshooting.

**Solution:**
Added logs volume mount to `docker-compose.yaml`:
```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./scripts:/opt/airflow/scripts
  - ./data:/opt/airflow/data
  - ./datamart:/opt/airflow/datamart
  - ./model_store:/opt/airflow/model_store
  - ./reports:/opt/airflow/reports
  - ./logs:/opt/airflow/logs  # Added this
```

Created the directory:
```powershell
New-Item -ItemType Directory -Path "./logs" -Force
```

Now logs are accessible at: `code/logs/dag_id=*/run_id=*/task_id=*/attempt=*.log`

---

## Final Working Configuration

### Docker Compose Structure
```yaml
services:
  - postgres (PostgreSQL 16-alpine)
  - airflow-init (DB migration + admin user creation)
  - airflow-webserver (Airflow 2.10.3, port 8080)
  - airflow-scheduler (LocalExecutor, parallelism=16)
```

### Key Environment Variables
```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: 16
AIRFLOW__CORE__DAG_CONCURRENCY: 8
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
AIRFLOW__CORE__FERNET_KEY: PAqBeGJLJTYFzVkOGHWIYXdLO7XdXz5yTdxAGJe9ezM=
```

### Volume Mounts
All data persisted on host machine:
- `./dags` → DAG Python files
- `./scripts` → Pipeline scripts
- `./data` → CSV input files (34 MB total)
- `./datamart/{bronze,silver,gold}` → Processed data layers
- `./model_store` → Trained models and metadata
- `./reports` → Performance dashboard PNG
- `./logs` → Airflow task execution logs

---

## Successful Pipeline Execution

### Training DAG Results
**3 Models Trained:**
| Model | Train AUC | Test AUC |
|-------|-----------|----------|
| Logistic Regression | 0.7289 | 0.7241 |
| Random Forest | 0.9227 | 0.7895 |
| **Gradient Boosting** | 0.9041 | **0.7898** ✅ |

**Selected Model:** Gradient Boosting (best test AUC)

### All DAGs Completed Successfully
1. ✅ **ml_training_pipeline** - Duration: 59 seconds
2. ✅ **ml_inference_pipeline** - Duration: 4 seconds
3. ✅ **ml_monitoring_governance_pipeline** - Duration: 3 minutes

### Generated Artifacts
- Model files: `model_store/best_model_*.pkl`
- Label encoders: `model_store/label_encoders_*.pkl`
- Model metadata: `model_store/model_metadata_*.json`
- Predictions: `datamart/gold/model_predictions.parquet`
- Monitoring metrics: `datamart/gold/model_monitoring.parquet`
- Dashboard: `reports/performance_dashboard.png` (687 KB)

---

## How to Reset and Start Fresh

If you need to completely reset the pipeline:

```powershell
# Stop and remove all containers + database
docker-compose down -v

# Clean all generated files
Remove-Item -Recurse -Force ./logs/*
Remove-Item -Recurse -Force ./datamart/bronze/*
Remove-Item -Recurse -Force ./datamart/silver/*
Remove-Item -Recurse -Force ./datamart/gold/*
Remove-Item -Recurse -Force ./model_store/*
Remove-Item -Recurse -Force ./reports/*

# Start fresh
docker-compose up -d

# Wait for services to be healthy (~30 seconds)
# Then access UI at http://localhost:8080 (admin/admin)
```

---

## Key Learnings

1. **Airflow 3.x is not production-ready** for LocalExecutor in Docker - stick with 2.10.x
2. **Don't specify Airflow version in requirements.txt** - let the base image handle it
3. **Always use parameterized paths** in pipeline scripts - avoid `__file__` path calculations
4. **Make the code folder self-contained** - copy all data files into `code/` for portability
5. **Mount logs directory** for easy debugging without exec-ing into containers
6. **Test task dependencies carefully** - especially with BranchPythonOperator to avoid skipped tasks
7. **Feature columns must match exactly** between training and inference
8. **Use `docker-compose build --no-cache`** when changing Dockerfile base images

---

## Useful Commands

### Check DAG Status
```bash
docker exec ml_pipeline_scheduler airflow dags list
```

### Trigger a DAG Manually
```bash
docker exec ml_pipeline_scheduler airflow dags trigger ml_training_pipeline
```

### View Task Logs
```bash
docker exec ml_pipeline_scheduler cat /opt/airflow/logs/dag_id=ml_training_pipeline/run_id=*/task_id=train_evaluate_select_model/attempt=1.log
```

### Check Container Health
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Restart Services
```bash
docker-compose restart airflow-webserver airflow-scheduler
```

---

## Architecture Diagram

```
CSV Files (data/)
    ↓
Bronze Layer (raw parquet) - 4 parallel tasks
    ↓
Silver Layer (cleaned) - 4 parallel tasks
    ↓
Quality Check (remove flagged customers)
    ↓
Gold Layer (feature engineering)
    ↓
ML Training (3 models, select best)
    ↓
Model Store (best_model.pkl)
    ↓
Inference (generate predictions)
    ↓
Monitoring (calculate metrics + dashboard)
    ↓
Governance Gate (trigger retraining if needed)
```

---

## Contact & Support

For issues or questions, check:
1. Airflow UI Logs tab
2. `code/logs/` directory on host machine
3. This troubleshooting guide

**Version Info:**
- Airflow: 2.10.3
- Python: 3.11
- PySpark: 3.5.0
- PostgreSQL: 16-alpine
