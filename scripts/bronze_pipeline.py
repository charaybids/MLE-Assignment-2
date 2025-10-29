"""
Bronze Pipeline - Data Ingestion
Reads CSV files and saves as compressed parquet format
"""

import pandas as pd
import os
from pathlib import Path

def run_bronze_pipeline(data_path, output_path):
    """
    Ingest raw CSV files and convert to parquet format
    
    Args:
        data_path: Path to raw CSV files
        output_path: Path to save bronze parquet files
    """
    print("=" * 60)
    print("BRONZE LAYER: Data Ingestion")
    print("=" * 60)
    
    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    # File mappings
    files = {
        'features_attributes.csv': 'features_attributes.parquet',
        'features_financials.csv': 'features_financials.parquet',
        'feature_clickstream.csv': 'feature_clickstream.parquet',
        'lms_loan_daily.csv': 'LMS_loans.parquet',
    }
    
    for csv_file, parquet_file in files.items():
        csv_path = os.path.join(data_path, csv_file)
        parquet_path = os.path.join(output_path, parquet_file)
        
        if not os.path.exists(csv_path):
            print(f"⚠️  {csv_file} not found, skipping...")
            continue
        
        df = pd.read_csv(csv_path, dtype=str)
        try:
            df.to_parquet(parquet_path, compression='gzip', engine='pyarrow', index=False)
        except:
            # Fallback to fastparquet if pyarrow has DLL issues
            df.to_parquet(parquet_path, compression='gzip', engine='fastparquet', index=False)
        
        print(f"✓ {csv_file} → {parquet_file} ({len(df)} rows)")
    
    print(f"\n✅ Bronze pipeline completed: {len(files)} files processed")
    return True

if __name__ == "__main__":
    # For testing
    project_root = Path(__file__).parent.parent.parent
    run_bronze_pipeline(
        data_path=str(project_root / "data"),
        output_path=str(project_root / "datamart" / "bronze")
    )
