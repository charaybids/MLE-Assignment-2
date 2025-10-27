"""
DAG 2: ML Inference Pipeline
Runs: Daily at 3 AM
Purpose: Retrieve best model and make predictions on latest data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Set Airflow paths
PROJECT_ROOT = '/opt/airflow'
sys.path.insert(0, '/opt/airflow/scripts')

from ml_inference import run_ml_inference
from gold_pipeline import run_gold_pipeline

# Default arguments
default_args = {
    'owner': 'ml_engineering',
    'depends_on_past': False,
    'email': ['ml-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Define DAG
dag = DAG(
    dag_id='ml_inference_pipeline',
    default_args=default_args,
    description='Generate predictions using best model',
    schedule='0 3 * * *',  # Daily at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'inference', 'daily'],
)

# Task 1: Prepare Inference Data (optional - if new data needs feature engineering)
def prepare_inference_data(**context):
    """
    Prepare data for inference
    Option 1: If new data arrives daily, run gold pipeline again
    Option 2: If using existing gold features, just log
    """
    print("📊 Preparing inference data...")
    print("   Using latest gold features from datamart/gold/gold_features.parquet")
    
    # Optionally re-run gold pipeline if new data arrived
    # run_gold_pipeline(
    #     input_path=os.path.join(PROJECT_ROOT, 'datamart', 'silver'),
    #     output_path=os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
    # )
    
    return True

prepare_task = PythonOperator(
    task_id='prepare_inference_data',
    python_callable=prepare_inference_data,
    dag=dag,
)

# Task 2: Retrieve Best Model
def retrieve_best_model(**context):
    """Load latest model metadata"""
    import glob
    import json
    
    model_store = os.path.join(PROJECT_ROOT, 'model_store')
    metadata_files = glob.glob(os.path.join(model_store, 'model_metadata_*.json'))
    
    if not metadata_files:
        raise FileNotFoundError("No trained models found! Run ml_training_pipeline first.")
    
    latest_metadata = max(metadata_files, key=os.path.getctime)
    with open(latest_metadata, 'r') as f:
        metadata = json.load(f)
    
    print(f"📦 Retrieved model: {metadata['model_name']}")
    print(f"   Timestamp: {metadata['timestamp']}")
    print(f"   Test AUC: {metadata['test_auc']:.4f}")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='model_metadata', value=metadata)
    return metadata

retrieve_task = PythonOperator(
    task_id='retrieve_best_model',
    python_callable=retrieve_best_model,
    dag=dag,
)

# Task 3: Make Predictions
inference_task = PythonOperator(
    task_id='make_predictions',
    python_callable=run_ml_inference,
    op_kwargs={
        'gold_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
        'model_store_path': os.path.join(PROJECT_ROOT, 'model_store'),
        'output_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
    },
    dag=dag,
)

# Task 4: Store Predictions to Gold Layer (already done in inference_task)
def store_predictions_gold(**context):
    """Verify predictions stored successfully"""
    import pandas as pd
    
    pred_path = os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'model_predictions.parquet')
    
    if os.path.exists(pred_path):
        pred_df = pd.read_parquet(pred_path)
        print(f"✅ Predictions stored: {len(pred_df)} records")
        print(f"   Location: {pred_path}")
        print(f"   Predicted defaulters: {pred_df['prediction'].sum()}")
        print(f"   Average probability: {pred_df['prediction_proba'].mean():.4f}")
        return True
    else:
        raise FileNotFoundError(f"Predictions file not found at {pred_path}")

store_task = PythonOperator(
    task_id='store_predictions_gold',
    python_callable=store_predictions_gold,
    dag=dag,
)

# Define task dependencies
prepare_task >> retrieve_task >> inference_task >> store_task
