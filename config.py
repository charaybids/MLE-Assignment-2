"""
Configuration File - Pipeline Constants
Centralizes all configuration parameters
"""

import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data"
BRONZE_PATH = PROJECT_ROOT / "datamart" / "bronze"
SILVER_PATH = PROJECT_ROOT / "datamart" / "silver"
GOLD_PATH = PROJECT_ROOT / "datamart" / "gold"
MODEL_STORE_PATH = PROJECT_ROOT / "model_store"
REPORTS_PATH = PROJECT_ROOT / "reports"

# Gold layer parameters
PREDICTION_MOB = 3
OBSERVATION_MOB = 6
OVERDUE_THRESHOLD = 0

# Model training parameters
TEST_SIZE = 0.2
RANDOM_STATE = 42

# Model selection
MODELS = {
    'LogisticRegression': {
        'max_iter': 1000,
        'random_state': RANDOM_STATE
    },
    'RandomForest': {
        'n_estimators': 100,
        'random_state': RANDOM_STATE
    },
    'GradientBoosting': {
        'n_estimators': 100,
        'random_state': RANDOM_STATE
    }
}

# Feature configuration
SAFE_FEATURES = [
    'age',
    'fa_1', 'fa_2', 'fa_3', 'fa_4', 'fa_5',
    'fa_6', 'fa_7', 'fa_8', 'fa_9', 'fa_10',
    'avg_dpd_obs', 'max_dpd_obs', 'min_dpd_obs', 'std_dpd_obs',
    'sum_fe_10'
]

# Monitoring thresholds
MONITORING_THRESHOLDS = {
    'auc_roc': 0.70,
    'precision': 0.60,
    'recall': 0.50,
    'f1_score': 0.55,
    'psi_warning': 0.1,
    'psi_critical': 0.2
}

# Airflow parameters
AIRFLOW_SCHEDULE = '0 2 * * *'  # Daily at 2 AM
AIRFLOW_EMAIL = ['ml-team@company.com']
AIRFLOW_RETRIES = 2
AIRFLOW_RETRY_DELAY_MINUTES = 5
AIRFLOW_EXECUTION_TIMEOUT_HOURS = 2

# Spark configuration
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "16g"
SPARK_APP_NAME = "MLPipeline"

# File formats
PARQUET_COMPRESSION = 'gzip'
PARQUET_ENGINE = 'pyarrow'
