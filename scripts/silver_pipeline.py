"""
Silver Pipeline - Data Cleaning & Quality Control
Performs data type conversion, quality flagging, and customer filtering
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, DateType
import pandas as pd

def create_spark_session():
    """Initialize Spark session"""
    return (SparkSession.builder
            .master("local[*]")
            .appName("SilverPipeline")
            .config("spark.driver.memory", "16g")
            .getOrCreate())

def read_parquet(spark, path):
    """Read parquet file into Spark DataFrame"""
    return spark.read.parquet(path)

def write_parquet(df, path):
    """Write Spark DataFrame to single compressed parquet file"""
    df.toPandas().to_parquet(path, compression='gzip', engine='pyarrow', index=False)

def remove_flagged_customers(attributes_df, financials_df, loan_daily_df, clickstream_df):
    """Remove customers with quality flags"""
    flagged = (attributes_df.filter(F.col('quality_flag') == 1)
               .select('customer_id').distinct())
    
    attributes_clean = attributes_df.join(flagged, on='customer_id', how='left_anti')
    financials_clean = financials_df.join(flagged, on='customer_id', how='left_anti')
    loan_daily_clean = loan_daily_df.join(flagged, on='customer_id', how='left_anti')
    clickstream_clean = clickstream_df.join(flagged, on='customer_id', how='left_anti')
    
    return attributes_clean, financials_clean, loan_daily_clean, clickstream_clean

def run_silver_pipeline(input_path, output_path):
    """
    Clean and standardize bronze layer data
    
    Args:
        input_path: Path to bronze parquet files
        output_path: Path to save silver parquet files
    """
    print("=" * 60)
    print("SILVER LAYER: Data Cleaning")
    print("=" * 60)
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    spark = create_spark_session()
    
    # Load bronze data
    attributes_df = read_parquet(spark, os.path.join(input_path, 'features_attributes.parquet'))
    financials_df = read_parquet(spark, os.path.join(input_path, 'features_financials.parquet'))
    loan_daily_df = read_parquet(spark, os.path.join(input_path, 'LMS_loans.parquet'))
    clickstream_df = read_parquet(spark, os.path.join(input_path, 'feature_clickstream.parquet'))
    print("Bronze parquet files loaded")
    
    # --- PARSE DATES (d/M/yyyy format) ---
    print("\nParsing dates with format d/M/yyyy")
    attributes_df = attributes_df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    financials_df = financials_df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    loan_daily_df = loan_daily_df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy')) \
                                 .withColumn('loan_start_date', F.to_date('loan_start_date', 'd/M/yyyy'))
    clickstream_df = clickstream_df.withColumn('snapshot_date', F.to_date('snapshot_date', 'd/M/yyyy'))
    print("Parsed 5 date columns")
    
    # --- ATTRIBUTES CLEANING ---
    print("\nCleaning attributes dataset")
    attributes_df = attributes_df.withColumn('Occupation', 
        F.when(F.trim(F.col('Occupation')).isin('_______', '_'), None).otherwise(F.col('Occupation')))
    print("Replaced placeholders ('_______', '_') with NULL in Occupation")
    
    attributes_df = attributes_df.withColumn('Age', 
        F.when(F.trim(F.regexp_replace('Age', r'[^0-9]', '')) == '', None)
         .otherwise(F.regexp_replace('Age', r'[^0-9]', '').cast(IntegerType())))
    print("Cleaned Age with r\"[^0-9]\" → IntegerType")
    
    attributes_df = attributes_df \
        .withColumn('age_flag', F.when((F.col('Age') < 18) | (F.col('Age') > 100), 1).otherwise(0)) \
        .withColumn('ssn_flag', F.when(F.trim(F.col('SSN')).rlike(r'^\d{3}-\d{2}-\d{4}$'), 0).otherwise(1)) \
        .withColumn('data_quality_issue', F.when((F.col('age_flag') == 1) | (F.col('ssn_flag') == 1), 1).otherwise(0))
    print("Flagged invalid Age/SSN")
    
    # --- FINANCIALS CLEANING ---
    print("\nCleaning financials dataset")
    for col_name in ['Credit_Mix', 'Payment_of_Min_Amount', 'Payment_Behaviour']:
        financials_df = financials_df.withColumn(col_name,
            F.when(F.trim(F.col(col_name)).isin('_______', '_', 'NM', '!@9#%8'), None).otherwise(F.col(col_name)))
    print("Replaced placeholders ('_', 'NM', '!@9#%8') with NULL in 3 categorical columns")
    
    float_cols = ['Annual_Income', 'Monthly_Inhand_Salary', 'Outstanding_Debt', 
                  'Total_EMI_per_month', 'Amount_invested_monthly', 'Monthly_Balance',
                  'Changed_Credit_Limit', 'Interest_Rate', 'Credit_Utilization_Ratio']
    for col_name in float_cols:
        financials_df = financials_df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9.]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9.]', '').cast(FloatType())))
    print(f"Cleaned {len(float_cols)} columns with r\"[^0-9.]\" → FloatType")
    
    int_cols = ['Num_of_Loan', 'Num_Bank_Accounts', 'Num_Credit_Card', 
                'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Num_Credit_Inquiries']
    for col_name in int_cols:
        financials_df = financials_df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9]', '').cast(IntegerType())))
    print(f"Cleaned {len(int_cols)} columns with r\"[^0-9]\" → IntegerType")
    
    financials_df = financials_df \
        .withColumn('negative_financials_flag', F.when(
            (F.col('Annual_Income') < 0) | (F.col('Monthly_Inhand_Salary') < 0) | (F.col('Outstanding_Debt') < 0), 
            1).otherwise(0)) \
        .withColumn('data_quality_issue', F.col('negative_financials_flag'))
    print("Flagged negative financial values")
    
    # --- LOAN_DAILY CLEANING ---
    print("\nCleaning loan_daily dataset")
    loan_int_cols = ['tenure', 'installment_num', 'loan_amt', 'due_amt', 'paid_amt', 'overdue_amt', 'balance']
    for col_name in loan_int_cols:
        loan_daily_df = loan_daily_df.withColumn(col_name,
            F.when(F.trim(F.regexp_replace(col_name, r'[^0-9]', '')) == '', None)
             .otherwise(F.regexp_replace(col_name, r'[^0-9]', '').cast(IntegerType())))
    print(f"Cleaned {len(loan_int_cols)} columns with r\"[^0-9]\" → IntegerType")
    
    loan_daily_df = loan_daily_df \
        .withColumn('negative_loan_vals_flag', F.when(
            (F.col('loan_amt') < 0) | (F.col('due_amt') < 0) | (F.col('paid_amt') < 0) | (F.col('overdue_amt') < 0),
            1).otherwise(0)) \
        .withColumn('data_quality_issue', F.col('negative_loan_vals_flag'))
    print("Flagged negative loan values")
    
    # --- CLICKSTREAM CLEANING ---
    print("\nCleaning clickstream dataset")
    for i in range(1, 21):
        clickstream_df = clickstream_df.withColumn(f'fe_{i}',
            F.when(F.trim(F.regexp_replace(f'fe_{i}', r'[^0-9-]', '')) == '', None)
             .otherwise(F.regexp_replace(f'fe_{i}', r'[^0-9-]', '').cast(IntegerType())))
    print("Cleaned 20 features (fe_1 to fe_20) with r\"[^0-9-]\" → IntegerType")
    
    # --- FLAG CUSTOMERS ---
    print("\nIdentifying flagged customers")
    flagged_attr = attributes_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    flagged_fin = financials_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    flagged_loan = loan_daily_df.filter(F.col('data_quality_issue') == 1).select('Customer_ID')
    all_flagged = flagged_attr.union(flagged_fin).union(flagged_loan).distinct()
    
    total_customers = attributes_df.select('Customer_ID').union(
        financials_df.select('Customer_ID')).union(
        loan_daily_df.select('Customer_ID')).union(
        clickstream_df.select('Customer_ID')).distinct().count()
    flagged_count = all_flagged.count()
    pct = (flagged_count / total_customers * 100.0) if total_customers else 0.0
    print(f"Flagged {flagged_count}/{total_customers} customers ({pct:.2f}%) for removal")
    
    # --- REMOVE FLAGGED CUSTOMERS ---
    print("\nRemoving flagged customers")
    attributes_clean = attributes_df.drop('age_flag', 'ssn_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    financials_clean = financials_df.drop('negative_financials_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    loan_daily_clean = loan_daily_df.drop('negative_loan_vals_flag', 'data_quality_issue') \
        .join(all_flagged, on='Customer_ID', how='left_anti')
    clickstream_clean = clickstream_df.join(all_flagged, on='Customer_ID', how='left_anti')
    
    # Save to silver layer
    write_parquet(attributes_clean, os.path.join(output_path, 'attributes.parquet'))
    write_parquet(financials_clean, os.path.join(output_path, 'financials.parquet'))
    write_parquet(loan_daily_clean, os.path.join(output_path, 'loan_daily.parquet'))
    write_parquet(clickstream_clean, os.path.join(output_path, 'clickstream.parquet'))
    
    clean_count = attributes_clean.count()
    print(f"\n✅ Silver pipeline completed: {clean_count} clean customers")
    
    spark.stop()
    return True

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    run_silver_pipeline(
        input_path=str(project_root / "datamart" / "bronze"),
        output_path=str(project_root / "datamart" / "silver")
    )
