"""
DAG 1: ML Training Pipeline (PARALLEL PROCESSING)
Runs: Weekly (Sundays) or Manual Trigger
Purpose: Train, evaluate, and select best model

Architecture:
- Bronze: 4 parallel ingestion tasks
- Silver: 4 parallel cleaning tasks
- Gold: Join all + feature engineering
- Training: Train & select best model
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Set Airflow paths
PROJECT_ROOT = '/opt/airflow'
sys.path.insert(0, '/opt/airflow/scripts')

from gold_pipeline import run_gold_pipeline
from ml_training import run_ml_training

# Default arguments
default_args = {
    'owner': 'ml_engineering',
    'depends_on_past': False,
    'email': ['ml-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

# Define DAG
dag = DAG(
    dag_id='ml_training_pipeline',
    default_args=default_args,
    description='Train ML models with parallel Bronze/Silver processing',
    schedule='0 2 * * 0',  # Weekly on Sundays at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'training', 'weekly', 'parallel'],
)

# ============================================================================
# BRONZE LAYER - PARALLEL INGESTION (4 tasks run simultaneously)
# ============================================================================

def ingest_csv_to_parquet(csv_file, parquet_file, **context):
    """Generic ingestion function for any CSV file"""
    from pathlib import Path
    
    data_path = os.path.join(PROJECT_ROOT, 'data', csv_file)
    output_path = os.path.join(PROJECT_ROOT, 'datamart', 'bronze', parquet_file)
    
    # Create output directory
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 Ingesting: {csv_file}")
    df = pd.read_csv(data_path, dtype=str)
    
    try:
        df.to_parquet(output_path, compression='gzip', engine='pyarrow', index=False)
    except:
        df.to_parquet(output_path, compression='gzip', engine='fastparquet', index=False)
    
    print(f"✅ Saved: {parquet_file} ({len(df)} rows)")
    return output_path

# Bronze Tasks - Run in Parallel
ingest_clickstream = PythonOperator(
    task_id='ingest_feature_clickstream',
    python_callable=ingest_csv_to_parquet,
    op_kwargs={'csv_file': 'feature_clickstream.csv', 'parquet_file': 'feature_clickstream.parquet'},
    dag=dag,
)

ingest_attributes = PythonOperator(
    task_id='ingest_features_attributes',
    python_callable=ingest_csv_to_parquet,
    op_kwargs={'csv_file': 'features_attributes.csv', 'parquet_file': 'features_attributes.parquet'},
    dag=dag,
)

ingest_financials = PythonOperator(
    task_id='ingest_features_financials',
    python_callable=ingest_csv_to_parquet,
    op_kwargs={'csv_file': 'features_financials.csv', 'parquet_file': 'features_financials.parquet'},
    dag=dag,
)

ingest_lms = PythonOperator(
    task_id='ingest_lms_loan_daily',
    python_callable=ingest_csv_to_parquet,
    op_kwargs={'csv_file': 'lms_loan_daily.csv', 'parquet_file': 'LMS_loans.parquet'},
    dag=dag,
)

# ============================================================================
# SILVER LAYER - PARALLEL CLEANING (4 tasks run simultaneously)
# ============================================================================

def clean_clickstream(**context):
    """Clean clickstream data independently"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    
    spark = SparkSession.builder.master("local[*]").appName("CleanClickstream").config("spark.driver.memory", "8g").getOrCreate()
    
    input_path = os.path.join(PROJECT_ROOT, 'datamart', 'bronze', 'feature_clickstream.parquet')
    output_path = os.path.join(PROJECT_ROOT, 'datamart', 'silver', 'clickstream.parquet')
    
    print("🧹 Cleaning clickstream data...")
    df = spark.read.parquet(input_path)
    
    # Parse date
    df = df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    
    # Clean 20 features (signed integers)
    for i in range(1, 21):
        df = df.withColumn(f'fe_{i}',
            F.when(F.trim(F.regexp_replace(f'fe_{i}', r'[^0-9-]', '')) == '', None)
             .otherwise(F.regexp_replace(f'fe_{i}', r'[^0-9-]', '').cast(IntegerType())))
    
    # Save
    df.toPandas().to_parquet(output_path, compression='gzip', engine='pyarrow', index=False)
    
    print(f"✅ Cleaned clickstream: {df.count()} rows")
    spark.stop()
    return output_path

def clean_attributes(**context):
    """Clean attributes data independently"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    
    spark = SparkSession.builder.master("local[*]").appName("CleanAttributes").config("spark.driver.memory", "8g").getOrCreate()
    
    input_path = os.path.join(PROJECT_ROOT, 'datamart', 'bronze', 'features_attributes.parquet')
    output_path = os.path.join(PROJECT_ROOT, 'datamart', 'silver', 'attributes.parquet')
    
    print("🧹 Cleaning attributes data...")
    df = spark.read.parquet(input_path)
    
    # Parse date
    df = df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    
    # Clean Occupation
    df = df.withColumn('Occupation', 
        F.when(F.trim(F.col('Occupation')).isin('_______', '_'), None).otherwise(F.col('Occupation')))
    
    # Clean Age
    df = df.withColumn('Age', 
        F.when(F.trim(F.regexp_replace('Age', r'[^0-9]', '')) == '', None)
         .otherwise(F.regexp_replace('Age', r'[^0-9]', '').cast(IntegerType())))
    
    # Quality flags
    df = df.withColumn('age_flag', F.when((F.col('Age') < 18) | (F.col('Age') > 100), 1).otherwise(0))
    df = df.withColumn('ssn_flag', F.when(F.trim(F.col('SSN')).rlike(r'^\d{3}-\d{2}-\d{4}$'), 0).otherwise(1))
    df = df.withColumn('data_quality_issue', F.when((F.col('age_flag') == 1) | (F.col('ssn_flag') == 1), 1).otherwise(0))
    
    # Drop quality flag columns
    df = df.drop('age_flag', 'ssn_flag', 'data_quality_issue')
    
    # Save
    df.toPandas().to_parquet(output_path, compression='gzip', engine='pyarrow', index=False)
    
    print(f"✅ Cleaned attributes: {df.count()} rows")
    spark.stop()
    return output_path

def clean_financials(**context):
    """Clean financials data independently"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, FloatType
    
    spark = SparkSession.builder.master("local[*]").appName("CleanFinancials").config("spark.driver.memory", "8g").getOrCreate()
    
    input_path = os.path.join(PROJECT_ROOT, 'datamart', 'bronze', 'features_financials.parquet')
    output_path = os.path.join(PROJECT_ROOT, 'datamart', 'silver', 'financials.parquet')
    
    print("🧹 Cleaning financials data...")
    df = spark.read.parquet(input_path)
    
    # Parse date
    df = df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    
    # Clean categorical columns
    for col_name in ['Credit_Mix', 'Payment_of_Min_Amount', 'Payment_Behaviour']:
        df = df.withColumn(col_name,
            F.when(F.trim(F.col(col_name)).isin('_______', '_', 'NM', '!@9#%8'), None).otherwise(F.col(col_name)))
    
    # Clean float columns
    float_cols = ['Annual_Income', 'Monthly_Inhand_Salary', 'Outstanding_Debt', 
                  'Total_EMI_per_month', 'Amount_invested_monthly', 'Monthly_Balance',
                  'Changed_Credit_Limit', 'Interest_Rate', 'Credit_Utilization_Ratio']
    for col_name in float_cols:
        df = df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9.]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9.]', '').cast(FloatType())))
    
    # Clean integer columns
    int_cols = ['Num_of_Loan', 'Num_Bank_Accounts', 'Num_Credit_Card', 
                'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Num_Credit_Inquiries']
    for col_name in int_cols:
        df = df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9]', '').cast(IntegerType())))
    
    # Quality flag
    df = df.withColumn('negative_financials_flag', F.when(
        (F.col('Annual_Income') < 0) | (F.col('Monthly_Inhand_Salary') < 0) | (F.col('Outstanding_Debt') < 0), 
        1).otherwise(0))
    df = df.withColumn('data_quality_issue', F.col('negative_financials_flag'))
    
    # Drop quality flags
    df = df.drop('negative_financials_flag', 'data_quality_issue')
    
    # Save
    df.toPandas().to_parquet(output_path, compression='gzip', engine='pyarrow', index=False)
    
    print(f"✅ Cleaned financials: {df.count()} rows")
    spark.stop()
    return output_path

def clean_loan_daily(**context):
    """Clean loan daily data independently"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    
    spark = SparkSession.builder.master("local[*]").appName("CleanLoanDaily").config("spark.driver.memory", "8g").getOrCreate()
    
    input_path = os.path.join(PROJECT_ROOT, 'datamart', 'bronze', 'LMS_loans.parquet')
    output_path = os.path.join(PROJECT_ROOT, 'datamart', 'silver', 'loan_daily.parquet')
    
    print("🧹 Cleaning loan daily data...")
    df = spark.read.parquet(input_path)
    
    # Parse dates
    df = df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    df = df.withColumn('loan_start_date', F.to_date('loan_start_date', 'd/M/yyyy'))
    
    # Clean integer columns
    loan_int_cols = ['tenure', 'installment_num', 'loan_amt', 'due_amt', 'paid_amt', 'overdue_amt', 'balance']
    for col_name in loan_int_cols:
        df = df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9]', '').cast(IntegerType())))
    
    # Quality flag
    df = df.withColumn('negative_loan_vals_flag', F.when(
        (F.col('loan_amt') < 0) | (F.col('due_amt') < 0) | (F.col('paid_amt') < 0) | (F.col('overdue_amt') < 0),
        1).otherwise(0))
    df = df.withColumn('data_quality_issue', F.col('negative_loan_vals_flag'))
    
    # Drop quality flags
    df = df.drop('negative_loan_vals_flag', 'data_quality_issue')
    
    # Save
    df.toPandas().to_parquet(output_path, compression='gzip', engine='pyarrow', index=False)
    
    print(f"✅ Cleaned loan daily: {df.count()} rows")
    spark.stop()
    return output_path

# Silver Tasks - Run in Parallel
clean_clickstream_task = PythonOperator(
    task_id='clean_clickstream',
    python_callable=clean_clickstream,
    dag=dag,
)

clean_attributes_task = PythonOperator(
    task_id='clean_attributes',
    python_callable=clean_attributes,
    dag=dag,
)

clean_financials_task = PythonOperator(
    task_id='clean_financials',
    python_callable=clean_financials,
    dag=dag,
)

clean_loan_daily_task = PythonOperator(
    task_id='clean_loan_daily',
    python_callable=clean_loan_daily,
    dag=dag,
)

# ============================================================================
# QUALITY CHECK - REMOVE FLAGGED CUSTOMERS
# ============================================================================

def remove_flagged_customers(**context):
    """Remove customers with quality issues from all datasets"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    spark = SparkSession.builder.master("local[*]").appName("RemoveFlagged").config("spark.driver.memory", "8g").getOrCreate()
    
    silver_path = os.path.join(PROJECT_ROOT, 'datamart', 'silver')
    
    # Load all silver datasets
    attributes_df = spark.read.parquet(os.path.join(silver_path, 'attributes.parquet'))
    financials_df = spark.read.parquet(os.path.join(silver_path, 'financials.parquet'))
    loan_daily_df = spark.read.parquet(os.path.join(silver_path, 'loan_daily.parquet'))
    clickstream_df = spark.read.parquet(os.path.join(silver_path, 'clickstream.parquet'))
    
    # Re-calculate quality flags
    attributes_df = attributes_df \
        .withColumn('age_flag', F.when((F.col('Age') < 18) | (F.col('Age') > 100) | F.col('Age').isNull(), 1).otherwise(0)) \
        .withColumn('ssn_flag', F.when(F.trim(F.col('SSN')).rlike(r'^\d{3}-\d{2}-\d{4}$'), 0).otherwise(1)) \
        .withColumn('data_quality_issue', F.when((F.col('age_flag') == 1) | (F.col('ssn_flag') == 1), 1).otherwise(0))
    
    financials_df = financials_df \
        .withColumn('negative_financials_flag', F.when(
            (F.col('Annual_Income') < 0) | (F.col('Monthly_Inhand_Salary') < 0) | (F.col('Outstanding_Debt') < 0), 
            1).otherwise(0)) \
        .withColumn('data_quality_issue', F.col('negative_financials_flag'))
    
    loan_daily_df = loan_daily_df \
        .withColumn('negative_loan_vals_flag', F.when(
            (F.col('loan_amt') < 0) | (F.col('due_amt') < 0) | (F.col('paid_amt') < 0) | (F.col('overdue_amt') < 0),
            1).otherwise(0)) \
        .withColumn('data_quality_issue', F.col('negative_loan_vals_flag'))
    
    # Identify all flagged customers
    flagged_attr = attributes_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    flagged_fin = financials_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    flagged_loan = loan_daily_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    all_flagged = flagged_attr.union(flagged_fin).union(flagged_loan).distinct()
    
    # Calculate stats
    total_customers = attributes_df.select('Customer_ID').distinct().count()
    flagged_count = all_flagged.count()
    pct = (flagged_count / total_customers * 100.0) if total_customers else 0.0
    
    print(f"\n🚩 Quality Check Results:")
    print(f"   Total customers: {total_customers}")
    print(f"   Flagged customers: {flagged_count} ({pct:.2f}%)")
    
    # Remove flagged customers from all datasets
    attributes_clean = attributes_df.drop('age_flag', 'ssn_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    financials_clean = financials_df.drop('negative_financials_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    loan_daily_clean = loan_daily_df.drop('negative_loan_vals_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    clickstream_clean = clickstream_df.join(all_flagged, on='Customer_ID', how='left_anti')
    
    # Overwrite silver files with cleaned data
    attributes_clean.toPandas().to_parquet(os.path.join(silver_path, 'attributes.parquet'), 
                                           compression='gzip', engine='pyarrow', index=False)
    financials_clean.toPandas().to_parquet(os.path.join(silver_path, 'financials.parquet'), 
                                           compression='gzip', engine='pyarrow', index=False)
    loan_daily_clean.toPandas().to_parquet(os.path.join(silver_path, 'loan_daily.parquet'), 
                                           compression='gzip', engine='pyarrow', index=False)
    clickstream_clean.toPandas().to_parquet(os.path.join(silver_path, 'clickstream.parquet'), 
                                            compression='gzip', engine='pyarrow', index=False)
    
    clean_count = attributes_clean.count()
    print(f"✅ Removed {flagged_count} customers, {clean_count} remaining")
    
    spark.stop()
    return clean_count

quality_check_task = PythonOperator(
    task_id='remove_flagged_customers',
    python_callable=remove_flagged_customers,
    dag=dag,
)

# ============================================================================
# GOLD LAYER - JOIN ALL & FEATURE ENGINEERING
# ============================================================================

gold_task = PythonOperator(
    task_id='gold_feature_engineering',
    python_callable=run_gold_pipeline,
    op_kwargs={
        'input_path': os.path.join(PROJECT_ROOT, 'datamart', 'silver'),
        'output_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
    },
    dag=dag,
)

# ============================================================================
# ML TRAINING
# ============================================================================

training_task = PythonOperator(
    task_id='train_evaluate_select_model',
    python_callable=run_ml_training,
    op_kwargs={
        'gold_path': os.path.join(PROJECT_ROOT, 'datamart', 'gold'),
        'model_store_path': os.path.join(PROJECT_ROOT, 'model_store'),
    },
    dag=dag,
)

def store_best_model_metadata(**context):
    """Log successful model storage"""
    print("✅ Best model stored in model_store/")
    print("   - Model artifact: best_model_YYYYMMDD_HHMMSS.pkl")
    print("   - Label encoders: label_encoders_YYYYMMDD_HHMMSS.pkl")
    print("   - Metadata: model_metadata_YYYYMMDD_HHMMSS.json")
    return True

store_task = PythonOperator(
    task_id='store_best_model',
    python_callable=store_best_model_metadata,
    dag=dag,
)

# ============================================================================
# DEFINE TASK DEPENDENCIES (PARALLEL STRUCTURE)
# ============================================================================

# Bronze ingestion (4 parallel tasks)
# No dependencies - all start simultaneously

# Silver cleaning (4 parallel tasks, each depends on its bronze task)
ingest_clickstream >> clean_clickstream_task
ingest_attributes >> clean_attributes_task
ingest_financials >> clean_financials_task
ingest_lms >> clean_loan_daily_task

# Quality check (waits for all 4 silver cleaning tasks)
[clean_clickstream_task, clean_attributes_task, clean_financials_task, clean_loan_daily_task] >> quality_check_task

# Gold feature engineering (waits for quality check)
quality_check_task >> gold_task

# Training (waits for gold)
gold_task >> training_task >> store_task
