"""
ML Monitoring Pipeline - Performance Tracking & Drift Detection
Monitors model metrics, calculates drift, and tracks monthly performance
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    accuracy_score, confusion_matrix
)

def calculate_psi(expected, actual, bins=10):
    """Calculate Population Stability Index"""
    expected_percents = np.histogram(expected, bins=bins)[0] / len(expected)
    actual_percents = np.histogram(actual, bins=bins)[0] / len(actual)
    
    expected_percents = np.where(expected_percents == 0, 0.0001, expected_percents)
    actual_percents = np.where(actual_percents == 0, 0.0001, actual_percents)
    
    psi = np.sum((actual_percents - expected_percents) * np.log(actual_percents / expected_percents))
    return psi

def run_ml_monitoring(predictions_path, output_path):
    """
    Monitor model performance and detect drift
    
    Args:
        predictions_path: Path to model predictions
        output_path: Path to save monitoring results
    """
    print("=" * 60)
    print("ML MONITORING: Performance Tracking")
    print("=" * 60)
    
    # Load predictions
    predictions_df = pd.read_parquet(predictions_path)
    
    # Calculate overall metrics
    y_true = predictions_df['actual_label']
    y_pred = predictions_df['prediction']
    y_proba = predictions_df['prediction_proba']
    
    metrics = {
        'auc_roc': roc_auc_score(y_true, y_proba),
        'accuracy': accuracy_score(y_true, y_pred),
        'precision': precision_score(y_true, y_pred),
        'recall': recall_score(y_true, y_pred),
        'f1_score': f1_score(y_true, y_pred),
    }
    
    cm = confusion_matrix(y_true, y_pred)
    
    print("\n📊 Overall Metrics:")
    for metric, value in metrics.items():
        print(f"   {metric}: {value:.4f}")
    
    print(f"\n   Confusion Matrix:")
    print(f"   TN: {cm[0,0]}, FP: {cm[0,1]}")
    print(f"   FN: {cm[1,0]}, TP: {cm[1,1]}")
    
    # Calculate PSI (drift)
    baseline_proba = y_proba[:len(y_proba)//2]  # First half as baseline
    current_proba = y_proba[len(y_proba)//2:]   # Second half as current
    
    psi = calculate_psi(baseline_proba, current_proba)
    drift_status = "No Drift" if psi < 0.1 else "Warning" if psi < 0.2 else "Critical Drift"
    
    print(f"\n📈 Drift Detection:")
    print(f"   PSI: {psi:.4f} ({drift_status})")
    
    # Monthly performance simulation (use customer_id groups)
    n_customers = len(predictions_df)
    predictions_df['month'] = (predictions_df.index // (n_customers // 12)) % 12 + 1
    
    monthly_metrics = []
    for month in predictions_df['month'].unique():
        month_data = predictions_df[predictions_df['month'] == month]
        if len(month_data) > 0:
            monthly_metrics.append({
                'month': month,
                'auc': roc_auc_score(month_data['actual_label'], month_data['prediction_proba']),
                'precision': precision_score(month_data['actual_label'], month_data['prediction']),
                'recall': recall_score(month_data['actual_label'], month_data['prediction']),
                'f1': f1_score(month_data['actual_label'], month_data['prediction']),
                'sample_size': len(month_data)
            })
    
    monthly_df = pd.DataFrame(monthly_metrics)
    
    # Save monitoring results
    monitoring_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'overall_metrics': metrics,
        'confusion_matrix': cm.tolist(),
        'psi': psi,
        'drift_status': drift_status,
    }
    
    monitoring_df = pd.DataFrame([monitoring_data])
    monitoring_df.to_parquet(
        os.path.join(output_path, 'model_monitoring.parquet'),
        compression='gzip', engine='pyarrow', index=False
    )
    
    monthly_df.to_parquet(
        os.path.join(output_path, 'monthly_performance.parquet'),
        compression='gzip', engine='pyarrow', index=False
    )
    
    print(f"\n✅ Monitoring completed: Results saved to {output_path}")
    
    # Alert if performance degraded
    if metrics['auc_roc'] < 0.70 or psi >= 0.2:
        print("\n⚠️  WARNING: Model performance or drift requires attention!")
    
    return True

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    run_ml_monitoring(
        predictions_path=str(project_root / "datamart" / "gold" / "model_predictions.parquet"),
        output_path=str(project_root / "datamart" / "gold")
    )
