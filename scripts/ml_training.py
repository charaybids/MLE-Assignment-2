"""
ML Training Pipeline - Model Training & Selection
Trains multiple models and selects best based on AUC
"""

import os
import pandas as pd
import pickle
import json
from pathlib import Path
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, classification_report

def run_ml_training(gold_path, model_store_path):
    """
    Train multiple ML models and select best performer
    
    Args:
        gold_path: Path to gold features
        model_store_path: Path to save trained models
    """
    print("=" * 60)
    print("ML TRAINING: Model Development")
    print("=" * 60)
    
    Path(model_store_path).mkdir(parents=True, exist_ok=True)
    
    # Load gold features
    gold_df = pd.read_parquet(os.path.join(gold_path, 'gold_features.parquet'))
    print(f"Loaded {len(gold_df)} records from gold features")
    
    # Prepare features and target
    feature_cols = ['Credit_History_Months', 'Age', 'Monthly_Inhand_Salary', 'loan_amt', 'tenure',
                    'Interest_Rate', 'fe_10_mean', 'fe_10_std', 'Savings_Ratio', 'DTI',
                    'Num_Bank_Accounts', 'Num_Credit_Card', 'Amount_invested_monthly']
    
    categorical_cols = ['Credit_Mix', 'Occupation']
    target_col = 'label'
    
    # Handle categorical features
    label_encoders = {}
    for col in categorical_cols:
        le = LabelEncoder()
        gold_df[col] = le.fit_transform(gold_df[col].fillna('Unknown'))
        label_encoders[col] = le
        feature_cols.append(col)
    
    # Handle missing values
    gold_df[feature_cols] = gold_df[feature_cols].fillna(gold_df[feature_cols].median())
    
    X = gold_df[feature_cols]
    y = gold_df[target_col]
    
    # Train-test split (80-20)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    print(f"Train: {len(X_train)} samples, Test: {len(X_test)} samples")
    print(f"Train default rate: {y_train.mean():.2%}, Test default rate: {y_test.mean():.2%}")
    
    # Define models to train
    models = {
        'logistic_regression': LogisticRegression(max_iter=1000, random_state=42, class_weight='balanced'),
        'random_forest': RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced'),
        'gradient_boosting': GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42)
    }
    
    # Train and evaluate models
    model_results = {}
    for name, model in models.items():
        print(f"\nTraining {name}...")
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        y_proba_train = model.predict_proba(X_train)[:, 1]
        y_proba_test = model.predict_proba(X_test)[:, 1]
        
        # Metrics
        train_auc = roc_auc_score(y_train, y_proba_train)
        test_auc = roc_auc_score(y_test, y_proba_test)
        
        model_results[name] = {
            'model': model,
            'train_auc': train_auc,
            'test_auc': test_auc,
            'y_pred_test': y_pred_test,
            'y_proba_test': y_proba_test
        }
        
        print(f"  Train AUC: {train_auc:.4f}, Test AUC: {test_auc:.4f}")
    
    # Select best model by test AUC
    best_model_name = max(model_results.keys(), key=lambda k: model_results[k]['test_auc'])
    best_model = model_results[best_model_name]['model']
    best_test_auc = model_results[best_model_name]['test_auc']
    
    print(f"\nBest model: {best_model_name} (Test AUC: {best_test_auc:.4f})")
    
    # Save best model
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_metadata = {
        'model_name': best_model_name,
        'timestamp': timestamp,
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'train_auc': float(model_results[best_model_name]['train_auc']),
        'test_auc': float(best_test_auc),
        'feature_columns': feature_cols,
        'label_encoders': {k: v.classes_.tolist() for k, v in label_encoders.items()}
    }
    
    # Save with pickle instead of joblib for compatibility
    with open(os.path.join(model_store_path, f'best_model_{timestamp}.pkl'), 'wb') as f:
        pickle.dump(best_model, f)
    with open(os.path.join(model_store_path, f'label_encoders_{timestamp}.pkl'), 'wb') as f:
        pickle.dump(label_encoders, f)
    with open(os.path.join(model_store_path, f'model_metadata_{timestamp}.json'), 'w') as f:
        json.dump(model_metadata, f, indent=2)
    
    # Save test data for monitoring
    test_data = X_test.copy()
    test_data['true_label'] = y_test.values
    test_data['predicted_label'] = model_results[best_model_name]['y_pred_test']
    test_data['predicted_proba'] = model_results[best_model_name]['y_proba_test']
    test_data.to_parquet(os.path.join(model_store_path, f'test_predictions_{timestamp}.parquet'), 
                         compression='gzip', engine='pyarrow', index=False)
    
    print(f"\n✅ Model training completed: Artifacts saved to {model_store_path}")
    return True

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    run_ml_training(
        gold_path=str(project_root / "datamart" / "gold"),
        model_store_path=str(project_root / "model_store")
    )
