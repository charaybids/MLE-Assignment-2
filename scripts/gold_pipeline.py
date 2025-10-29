"""
Gold Pipeline - Feature Engineering
Creates model-ready features with temporal constraints
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import pandas as pd

PREDICTION_MOB = 3
OBSERVATION_MOB = 6
OVERDUE_THRESHOLD = 0

def create_spark_session():
    """Initialize Spark session"""
    return (SparkSession.builder
            .master("local[*]")
            .appName("GoldPipeline")
            .config("spark.driver.memory", "16g")
            .getOrCreate())

def read_parquet(spark, path):
    """Read parquet file into Spark DataFrame"""
    return spark.read.parquet(path)

def write_parquet(df, path):
    """Write Spark DataFrame to single compressed parquet file"""
    df.toPandas().to_parquet(path, compression='gzip', engine='pyarrow', index=False)

def run_gold_pipeline(input_path, output_path):
    """
    Engineer features with temporal constraints and create labels
    
    Args:
        input_path: Path to silver parquet files
        output_path: Path to save gold parquet files
    """
    print("=" * 60)
    print("GOLD LAYER: Feature Engineering")
    print("=" * 60)
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    spark = create_spark_session()
    
    # Load silver data
    attributes_df = read_parquet(spark, os.path.join(input_path, 'attributes.parquet'))
    financials_df = read_parquet(spark, os.path.join(input_path, 'financials.parquet'))
    loan_daily_df = read_parquet(spark, os.path.join(input_path, 'loan_daily.parquet'))
    clickstream_df = read_parquet(spark, os.path.join(input_path, 'clickstream.parquet'))
    print("Silver files loaded")
    
    # Create label store
    loan_info = loan_daily_df.select("loan_id", "Customer_ID", "loan_start_date").distinct()
    labels_df = loan_info \
        .withColumn("prediction_date", F.add_months(F.col("loan_start_date"), PREDICTION_MOB)) \
        .withColumn("observation_date", F.add_months(F.col("loan_start_date"), OBSERVATION_MOB))
    
    default_events = loan_daily_df.filter(
        (F.col("installment_num") >= PREDICTION_MOB) & 
        (F.col("installment_num") <= OBSERVATION_MOB) &
        (F.col("overdue_amt") > OVERDUE_THRESHOLD)
    ).select("loan_id").distinct().withColumn("defaulted_flag", F.lit(1))
    
    label_store = labels_df.join(default_events, "loan_id", "left") \
        .withColumn("label", F.when(F.col("defaulted_flag").isNotNull(), 1).otherwise(0)) \
        .select("Customer_ID", "loan_id", "prediction_date", "observation_date", "label")
    print("Label store created")
    
    # Time-aware filtering
    loan_application = loan_daily_df.filter(F.col("installment_num") == 0) \
        .select("loan_id", "Customer_ID", "loan_amt", "tenure", "loan_start_date")
    
    clickstream_history = clickstream_df.join(
        loan_application.select("Customer_ID", "loan_start_date"), "Customer_ID"
    ).filter(F.col("snapshot_date") < F.col("loan_start_date"))
    
    loan_history = loan_daily_df.join(
        label_store.select("loan_id", "prediction_date"), "loan_id"
    ).filter((F.col("installment_num") >= 0) & (F.col("installment_num") < PREDICTION_MOB))
    print("Time-aware filtering applied")
    
    # Impute NULLs with median
    numeric_cols = ['Annual_Income', 'Monthly_Inhand_Salary', 'Num_Bank_Accounts', 'Num_Credit_Card',
                    'Interest_Rate', 'Num_of_Loan', 'Delay_from_due_date', 'Num_of_Delayed_Payment',
                    'Changed_Credit_Limit', 'Num_Credit_Inquiries', 'Outstanding_Debt',
                    'Credit_Utilization_Ratio', 'Total_EMI_per_month', 'Amount_invested_monthly', 'Monthly_Balance']
    
    for col_name in numeric_cols:
        median_val = financials_df.approxQuantile(col_name, [0.5], 0.01)[0]
        if median_val is not None:
            financials_df = financials_df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(median_val)))
    
    age_median = attributes_df.approxQuantile('Age', [0.5], 0.01)[0]
    if age_median is not None:
        attributes_df = attributes_df.withColumn('Age', F.coalesce(F.col('Age'), F.lit(age_median)))
    print("NULL imputation completed")
    
    # Aggregate loan history
    loan_agg = loan_history.groupBy("Customer_ID").agg(
        F.sum("paid_amt").alias("hist_total_paid"),
        F.sum("due_amt").alias("hist_total_due"),
        F.sum("overdue_amt").alias("hist_total_overdue_amount")
    ).withColumn("hist_Loan_Payment_Ratio", 
        F.when(F.col("hist_total_due") > 0, F.col("hist_total_paid") / F.col("hist_total_due")).otherwise(1.0))
    
    # Aggregate clickstream (fe_10 only - strongest predictor)
    clickstream_agg = clickstream_history.groupBy("Customer_ID").agg(
        F.mean("fe_10").alias("fe_10_mean"),
        F.stddev("fe_10").alias("fe_10_std")
    )
    print("Aggregations completed")
    
    # Get latest snapshots
    attributes_as_of = attributes_df.join(
        label_store.select("Customer_ID", "prediction_date"), "Customer_ID"
    ).filter(F.col("snapshot_date") <= F.col("prediction_date")) \
     .groupBy("Customer_ID").agg(F.max('snapshot_date').alias('latest_snapshot'))
    
    attributes_latest = attributes_df.join(
        attributes_as_of, 
        on=[attributes_df.Customer_ID == attributes_as_of.Customer_ID, 
            attributes_df.snapshot_date == attributes_as_of.latest_snapshot]
    ).select(attributes_df["*"])
    
    financials_as_of = financials_df.join(
        label_store.select("Customer_ID", "prediction_date"), "Customer_ID"
    ).filter(F.col("snapshot_date") <= F.col("prediction_date")) \
     .groupBy("Customer_ID").agg(F.max('snapshot_date').alias('latest_snapshot'))
    
    financials_latest = financials_df.join(
        financials_as_of, 
        on=[financials_df.Customer_ID == financials_as_of.Customer_ID, 
            financials_df.snapshot_date == financials_as_of.latest_snapshot]
    ).select(financials_df["*"])
    
    # Engineer features
    years_col = F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s+Years", 1).cast(IntegerType())
    months_col = F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s+Months", 1).cast(IntegerType())
    
    financials_features = financials_latest.withColumn(
        "Credit_History_Months", F.coalesce(years_col, F.lit(0)) * 12 + F.coalesce(months_col, F.lit(0))
    ).withColumn("DTI", F.col("Total_EMI_per_month") / F.col("Monthly_Inhand_Salary")
    ).withColumn("Savings_Ratio", F.col("Amount_invested_monthly") / F.col("Monthly_Inhand_Salary")
    ).withColumn("Monthly_Surplus", F.col("Monthly_Inhand_Salary") - F.col("Total_EMI_per_month") - F.col("Amount_invested_monthly")
    ).withColumn("Debt_to_Annual_Income", F.col("Outstanding_Debt") / F.col("Annual_Income"))
    print("Feature engineering completed")
    
    # Join all features
    gold_features = label_store \
        .join(loan_application.select("loan_id", "loan_amt", "tenure"), "loan_id", "inner") \
        .join(attributes_latest.select("Customer_ID", "Age", "Occupation"), "Customer_ID", "inner") \
        .join(financials_features.drop("snapshot_date"), "Customer_ID", "left") \
        .join(clickstream_agg, "Customer_ID", "left") \
        .join(loan_agg, "Customer_ID", "left")
    
    # Filter to 15 safe features
    safe_features = ['Credit_History_Months', 'Credit_Mix', 'Age', 'Monthly_Inhand_Salary', 'Occupation',
                     'loan_amt', 'tenure', 'Interest_Rate', 'fe_10_mean', 'fe_10_std',
                     'Savings_Ratio', 'DTI', 'Num_Bank_Accounts', 'Num_Credit_Card', 'Amount_invested_monthly']
    
    id_cols = ['Customer_ID', 'loan_id', 'prediction_date', 'observation_date', 'label']
    selected_cols = id_cols + [c for c in safe_features if c in gold_features.columns]
    gold_features_filtered = gold_features.select(*selected_cols)
    
    # Save outputs
    write_parquet(gold_features_filtered, os.path.join(output_path, 'gold_features.parquet'))
    write_parquet(label_store, os.path.join(output_path, 'label_store.parquet'))
    
    feature_count = gold_features_filtered.count()
    defaulter_count = label_store.filter(F.col('label') == 1).count()
    print(f"\n✅ Gold pipeline completed: {feature_count} records, {defaulter_count} defaulters")
    
    spark.stop()
    return True

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    run_gold_pipeline(
        input_path=str(project_root / "datamart" / "silver"),
        output_path=str(project_root / "datamart" / "gold")
    )
