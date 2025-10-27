"""
DAG 3: ML Monitoring & Governance Pipeline
Runs: Daily at 4 AM (after inference)
Purpose: Monitor performance, detect drift, trigger retraining if needed
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Set Airflow paths
PROJECT_ROOT = '/opt/airflow'
sys.path.insert(0, '/opt/airflow/scripts')

from ml_monitoring import run_ml_monitoring
from generate_report import generate_report

# Default arguments
default_args = {
    'owner': 'ml_engineering',
    'depends_on_past': False,
    'email': ['ml-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=30),
}

# Define DAG
dag = DAG(
    dag_id='ml_monitoring_governance_pipeline',
    default_args=default_args,
    description='Monitor model performance and trigger retraining if needed',
    schedule='0 4 * * *',  # Daily at 4 AM (after inference)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'monitoring', 'governance', 'daily'],
)

# Task 1: Fetch Predictions and Actuals
def fetch_predictions_and_actuals(**context):
    """Load predictions and actual labels"""
    import pandas as pd
    
    pred_path = os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'model_predictions.parquet')
    
    if not os.path.exists(pred_path):
        raise FileNotFoundError(f"Predictions not found at {pred_path}. Run inference pipeline first.")
    
    pred_df = pd.read_parquet(pred_path)
    
    print(f"📊 Fetched data for monitoring:")
    print(f"   Total records: {len(pred_df)}")
    print(f"   Predictions: {pred_df['prediction'].sum()} positives")
    print(f"   Actuals: {pred_df['actual_label'].sum()} positives")
    
    # Push to XCom
    context['ti'].xcom_push(key='record_count', value=len(pred_df))
    return True

fetch_task = PythonOperator(
    task_id='fetch_predictions_and_actuals',
    python_callable=fetch_predictions_and_actuals,
    dag=dag,
)

# Task 2: Calculate Monitoring Metrics
monitoring_task = PythonOperator(
    task_id='calculate_monitoring_metrics',
    python_callable=run_ml_monitoring,
    op_kwargs={
        'predictions_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'model_predictions.parquet'),
        'output_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
    },
    dag=dag,
)

# Task 3: Store Monitoring Results (already done in monitoring_task)
def store_monitoring_results_gold(**context):
    """Verify monitoring results stored"""
    import pandas as pd
    
    monitoring_path = os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'model_monitoring.parquet')
    monthly_path = os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'monthly_performance.parquet')
    
    if os.path.exists(monitoring_path):
        mon_df = pd.read_parquet(monitoring_path)
        print(f"✅ Monitoring results stored:")
        print(f"   Overall metrics: {monitoring_path}")
        print(f"   Monthly performance: {monthly_path}")
        
        # Get latest metrics
        latest = mon_df.iloc[-1]
        metrics = latest['overall_metrics']
        psi = latest['psi']
        
        context['ti'].xcom_push(key='latest_auc', value=metrics['auc_roc'])
        context['ti'].xcom_push(key='latest_psi', value=psi)
        
        return {
            'auc_roc': metrics['auc_roc'],
            'psi': psi,
            'drift_status': latest['drift_status']
        }
    else:
        raise FileNotFoundError(f"Monitoring results not found at {monitoring_path}")

store_results_task = PythonOperator(
    task_id='store_monitoring_results_gold',
    python_callable=store_monitoring_results_gold,
    dag=dag,
)

# Task 4: Visualize Dashboard
visualize_task = PythonOperator(
    task_id='visualize_monitoring_dashboard',
    python_callable=generate_report,
    op_kwargs={
        'gold_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
        'reports_path': os.path.join(PROJECT_ROOT, 'reports'),
    },
    dag=dag,
)

# Task 5: Model Governance Gate (Branch based on performance)
def model_governance_gate(**context):
    """
    Decision gate: Check if retraining is needed
    
    Triggers:
    - AUC drops below 0.70
    - PSI > 0.2 (critical drift)
    - Precision/Recall below thresholds
    """
    import pandas as pd
    
    monitoring_path = os.path.join(PROJECT_ROOT, 'datamart', 'gold', 'model_monitoring.parquet')
    mon_df = pd.read_parquet(monitoring_path)
    latest = mon_df.iloc[-1]
    
    metrics = latest['overall_metrics']
    psi = latest['psi']
    
    # Define thresholds
    THRESHOLDS = {
        'auc_roc': 0.70,
        'precision': 0.60,
        'recall': 0.50,
        'f1_score': 0.55,
        'psi_critical': 0.2,
    }
    
    # Check conditions
    triggers = []
    
    if metrics['auc_roc'] < THRESHOLDS['auc_roc']:
        triggers.append(f"AUC ({metrics['auc_roc']:.4f}) < {THRESHOLDS['auc_roc']}")
    
    if metrics['precision'] < THRESHOLDS['precision']:
        triggers.append(f"Precision ({metrics['precision']:.4f}) < {THRESHOLDS['precision']}")
    
    if metrics['recall'] < THRESHOLDS['recall']:
        triggers.append(f"Recall ({metrics['recall']:.4f}) < {THRESHOLDS['recall']}")
    
    if psi >= THRESHOLDS['psi_critical']:
        triggers.append(f"PSI ({psi:.4f}) >= {THRESHOLDS['psi_critical']} (Critical Drift)")
    
    if triggers:
        print("🔴 MODEL GOVERNANCE: RETRAINING REQUIRED")
        for trigger in triggers:
            print(f"   ⚠️  {trigger}")
        return 'trigger_retraining_dag'
    else:
        print("✅ MODEL GOVERNANCE: PERFORMANCE OK")
        print(f"   AUC: {metrics['auc_roc']:.4f}")
        print(f"   Precision: {metrics['precision']:.4f}")
        print(f"   Recall: {metrics['recall']:.4f}")
        print(f"   PSI: {psi:.4f}")
        return 'end_monitoring_ok'

governance_gate = BranchPythonOperator(
    task_id='model_governance_gate',
    python_callable=model_governance_gate,
    dag=dag,
)

# Task 6a: Trigger Retraining DAG (if performance degraded)
trigger_retraining = TriggerDagRunOperator(
    task_id='trigger_retraining_dag',
    trigger_dag_id='ml_training_pipeline',
    wait_for_completion=False,
    dag=dag,
)

# Task 6b: End Monitoring (if performance OK)
def end_monitoring_ok(**context):
    """Log successful monitoring completion"""
    print("✅ Monitoring completed - No retraining needed")
    return True

end_ok_task = PythonOperator(
    task_id='end_monitoring_ok',
    python_callable=end_monitoring_ok,
    dag=dag,
)

# Define task dependencies
fetch_task >> monitoring_task >> store_results_task >> visualize_task >> governance_gate
governance_gate >> [trigger_retraining, end_ok_task]

# Note: visualize_task runs BEFORE governance decision so dashboard is always generated
