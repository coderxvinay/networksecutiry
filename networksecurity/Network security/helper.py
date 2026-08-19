import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import VarianceThreshold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

def load_data(filepath='phising-data.csv'):
    df = pd.read_csv(filepath)
    return df

def preprocess_data(df):
    target_col = df.columns[-1]
    # Standardize target to 0 and 1 if it contains -1
    if df[target_col].min() == -1:
        df[target_col] = df[target_col].replace({-1: 0})
        
    X = df.drop(columns=[target_col])
    y = df[target_col]
    
    # Remove Constant Features (Zero Variance)
    var_thres = VarianceThreshold(threshold=0)
    var_thres.fit(X)
    constant_columns = [col for col in X.columns if col not in X.columns[var_thres.get_support()]]
    if len(constant_columns) > 0:
        X = X.drop(columns=constant_columns)
        
    # Handling Multicollinearity (Threshold: > 0.85)
    corr_features = X.corr().abs()
    upper = corr_features.where(np.triu(np.ones(corr_features.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if any(upper[column] > 0.85)]
    X = X.drop(columns=to_drop)
    
    return X, y, target_col

def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Using Tuned Random Forest Classifier as the robust baseline
    model = RandomForestClassifier(
        n_estimators=200, 
        max_depth=20, 
        min_samples_split=2, 
        min_samples_leaf=1, 
        random_state=42
    )
    model.fit(X_train_scaled, y_train)
    
    return model, scaler, X_test_scaled, y_test

def get_metrics(model, X_test_scaled, y_test):
    """Calculates classification metrics."""
    y_pred = model.predict(X_test_scaled)
    
    metrics = {
        "Accuracy": accuracy_score(y_test, y_pred),
        "Precision": precision_score(y_test, y_pred),
        "Recall": recall_score(y_test, y_pred),
        "F1 Score": f1_score(y_test, y_pred, average='weighted')
    }
    return metrics

def get_feature_importances(model, feature_names):
    """Extracts feature importances from the tree model."""
    importances = model.feature_importances_
    imp_df = pd.DataFrame({'Feature': feature_names, 'Importance': importances})
    imp_df = imp_df.sort_values(by='Importance', ascending=False).head(15) # Top 15
    return imp_df