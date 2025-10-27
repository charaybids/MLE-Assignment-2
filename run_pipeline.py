"""
Test Runner - Execute All Pipeline Stages
Runs the complete ML pipeline from Bronze to Monitoring
"""

import sys
import os
from pathlib import Path

# Add scripts directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "code" / "scripts"))

# Import pipeline functions
from bronze_pipeline import run_bronze_pipeline
from silver_pipeline import run_silver_pipeline
from gold_pipeline import run_gold_pipeline
from ml_training import run_ml_training
from ml_inference import run_ml_inference
from ml_monitoring import run_ml_monitoring
from generate_report import generate_report

def main():
    """Run complete ML pipeline"""
    print("\n" + "="*80)
    print("RUNNING COMPLETE ML PIPELINE")
    print("="*80 + "\n")
    
    base_path = project_root
    
    try:
        # Step 1: Bronze - Data Ingestion
        print("\n[1/7] Running Bronze Pipeline...")
        run_bronze_pipeline(
            data_path=str(base_path / "data"),
            output_path=str(base_path / "datamart" / "bronze")
        )
        
        # Step 2: Silver - Data Cleaning
        print("\n[2/7] Running Silver Pipeline...")
        run_silver_pipeline(
            input_path=str(base_path / "datamart" / "bronze"),
            output_path=str(base_path / "datamart" / "silver")
        )
        
        # Step 3: Gold - Feature Engineering
        print("\n[3/7] Running Gold Pipeline...")
        run_gold_pipeline(
            input_path=str(base_path / "datamart" / "silver"),
            output_path=str(base_path / "datamart" / "gold")
        )
        
        # Step 4: ML Training
        print("\n[4/7] Running ML Training...")
        run_ml_training(
            gold_path=str(base_path / "datamart" / "gold"),
            model_store_path=str(base_path / "model_store")
        )
        
        # Step 5: ML Inference
        print("\n[5/7] Running ML Inference...")
        run_ml_inference(
            gold_path=str(base_path / "datamart" / "gold"),
            model_store_path=str(base_path / "model_store"),
            output_path=str(base_path / "datamart" / "gold")
        )
        
        # Step 6: Model Monitoring
        print("\n[6/7] Running Model Monitoring...")
        run_ml_monitoring(
            predictions_path=str(base_path / "datamart" / "gold" / "model_predictions.parquet"),
            output_path=str(base_path / "datamart" / "gold")
        )
        
        # Step 7: Generate Report
        print("\n[7/7] Generating Performance Report...")
        generate_report()
        
        print("\n" + "="*80)
        print("✅ COMPLETE ML PIPELINE FINISHED SUCCESSFULLY!")
        print("="*80)
        print(f"\n📁 Outputs:")
        print(f"  - Data: {base_path / 'datamart'}")
        print(f"  - Models: {base_path / 'model_store'}")
        print(f"  - Reports: {base_path / 'reports'}")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
