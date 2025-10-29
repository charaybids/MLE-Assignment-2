"""
Generate Performance Report - Visualization Dashboard
Creates comprehensive performance charts and saves report
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.metrics import roc_curve, precision_recall_curve, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

def generate_report(gold_path='/opt/airflow/datamart/gold', reports_path='/opt/airflow/reports'):
    """Generate performance visualization dashboard"""
    print("=" * 60)
    print("REPORTING: Generating Performance Dashboard")
    print("=" * 60)
    
    # Load data
    predictions_df = pd.read_parquet(os.path.join(gold_path, "model_predictions.parquet"))
    monthly_df = pd.read_parquet(os.path.join(gold_path, "monthly_performance.parquet"))
    
    # Create figure
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('ML Model Performance Dashboard', fontsize=16, fontweight='bold')
    
    y_true = predictions_df['actual_label']
    y_pred = predictions_df['prediction']
    y_proba = predictions_df['prediction_proba']
    
    # 1. ROC Curve
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    axes[0, 0].plot(fpr, tpr, linewidth=2)
    axes[0, 0].plot([0, 1], [0, 1], 'k--', linewidth=1)
    axes[0, 0].set_xlabel('False Positive Rate')
    axes[0, 0].set_ylabel('True Positive Rate')
    axes[0, 0].set_title('ROC Curve')
    axes[0, 0].grid(True, alpha=0.3)
    
    # 2. Precision-Recall Curve
    precision, recall, _ = precision_recall_curve(y_true, y_proba)
    axes[0, 1].plot(recall, precision, linewidth=2)
    axes[0, 1].set_xlabel('Recall')
    axes[0, 1].set_ylabel('Precision')
    axes[0, 1].set_title('Precision-Recall Curve')
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. Confusion Matrix
    cm = confusion_matrix(y_true, y_pred)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0, 2])
    axes[0, 2].set_xlabel('Predicted')
    axes[0, 2].set_ylabel('Actual')
    axes[0, 2].set_title('Confusion Matrix')
    
    # 4. Prediction Distribution
    axes[1, 0].hist([y_proba[y_true == 0], y_proba[y_true == 1]], 
                    bins=30, label=['Non-Defaulter', 'Defaulter'], alpha=0.7)
    axes[1, 0].set_xlabel('Prediction Probability')
    axes[1, 0].set_ylabel('Frequency')
    axes[1, 0].set_title('Prediction Distribution')
    axes[1, 0].legend()
    
    # 5. Monthly AUC Trend
    axes[1, 1].plot(monthly_df['month'], monthly_df['auc'], marker='o', linewidth=2)
    axes[1, 1].axhline(y=0.70, color='r', linestyle='--', label='Threshold')
    axes[1, 1].set_xlabel('Month')
    axes[1, 1].set_ylabel('AUC Score')
    axes[1, 1].set_title('Monthly AUC Trend')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    # 6. Monthly Metrics Comparison
    axes[1, 2].plot(monthly_df['month'], monthly_df['precision'], marker='o', label='Precision')
    axes[1, 2].plot(monthly_df['month'], monthly_df['recall'], marker='s', label='Recall')
    axes[1, 2].plot(monthly_df['month'], monthly_df['f1'], marker='^', label='F1-Score')
    axes[1, 2].set_xlabel('Month')
    axes[1, 2].set_ylabel('Score')
    axes[1, 2].set_title('Monthly Metrics Comparison')
    axes[1, 2].legend()
    axes[1, 2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save report
    Path(reports_path).mkdir(parents=True, exist_ok=True)
    output_file = os.path.join(reports_path, 'performance_dashboard.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Report generated: {output_file}")
    
    plt.close()
    return True

if __name__ == "__main__":
    # For standalone testing
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    generate_report(
        gold_path=str(project_root / "datamart" / "gold"),
        reports_path=str(project_root / "reports")
    )
