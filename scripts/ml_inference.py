"""
ML Inference Pipeline - Generate Predictions
Loads best model and generates predictions on gold features
"""

import os
import pandas as pd
import pickle
import glob
from pathlib import Path

def run_ml_inference(gold_path, model_store_path, output_path):
    """
    Load best model and generate predictions
    
    Args:
        gold_path: Path to gold features
        model_store_path: Path to saved models
        output_path: Path to save predictions
    """
    print("=" * 60)
    print("ML INFERENCE: Generating Predictions")
    print("=" * 60)
    
    # Load latest model
    model_files = glob.glob(os.path.join(model_store_path, 'best_model_*.pkl'))
    if not model_files:
        raise FileNotFoundError("No trained model found!")
    
    latest_model_path = max(model_files, key=os.path.getctime)
    with open(latest_model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"Loaded model: {os.path.basename(latest_model_path)}")
    
    # Load label encoders
    encoder_files = glob.glob(os.path.join(model_store_path, 'label_encoders_*.pkl'))
    if encoder_files:
        latest_encoder_path = max(encoder_files, key=os.path.getctime)
        with open(latest_encoder_path, 'rb') as f:
            label_encoders = pickle.load(f)
        print(f"Loaded encoders: {os.path.basename(latest_encoder_path)}")
    else:
        label_encoders = {}
    
    # Load gold features
    gold_df = pd.read_parquet(os.path.join(gold_path, 'gold_features.parquet'))
    
    # Prepare features (must match training features)
    feature_cols = ['Credit_History_Months', 'Age', 'Monthly_Inhand_Salary', 'loan_amt', 'tenure',
                    'Interest_Rate', 'fe_10_mean', 'fe_10_std', 'Savings_Ratio', 'DTI',
                    'Num_Bank_Accounts', 'Num_Credit_Card', 'Amount_invested_monthly']
    
    categorical_cols = ['Credit_Mix', 'Occupation']
    
    # Apply label encoding to categorical features
    for col in categorical_cols:
        if col in label_encoders:
            gold_df[col] = label_encoders[col].transform(gold_df[col].fillna('Unknown'))
            feature_cols.append(col)
    
    # Handle missing values
    gold_df[feature_cols] = gold_df[feature_cols].fillna(0)
    
    X = gold_df[feature_cols]
    
    # Generate predictions
    predictions = model.predict(X)
    prediction_proba = model.predict_proba(X)[:, 1]
    
    # Create predictions dataframe
    predictions_df = pd.DataFrame({
        'Customer_ID': gold_df['Customer_ID'],
        'loan_id': gold_df['loan_id'],
        'prediction': predictions,
        'prediction_proba': prediction_proba,
        'actual_label': gold_df['label']
    })
    
    # Save predictions
    output_file = os.path.join(output_path, 'model_predictions.parquet')
    predictions_df.to_parquet(output_file, compression='gzip', engine='pyarrow', index=False)
    
    defaulter_predictions = predictions_df['prediction'].sum()
    print(f"\n✅ Inference completed: {len(predictions_df)} predictions generated")
    print(f"   Predicted defaulters: {defaulter_predictions}")
    
    return True

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    run_ml_inference(
        gold_path=str(project_root / "datamart" / "gold"),
        model_store_path=str(project_root / "model_store"),
        output_path=str(project_root / "datamart" / "gold")
    )
