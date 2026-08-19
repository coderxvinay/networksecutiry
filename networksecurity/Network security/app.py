import streamlit as st
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, roc_auc_score, ConfusionMatrixDisplay
import helper

st.set_page_config(page_title="Phishing Detection", page_icon="🛡️", layout="centered")

st.title("🛡️ Phishing Classification Model")
st.write("A basic dashboard showcasing the performance of our Random Forest model on the test dataset.")

@st.cache_data
def fetch_and_clean_data():
    df = helper.load_data('phising-data.csv')
    X, y, target_col = helper.preprocess_data(df)
    return df, X, y, target_col

@st.cache_resource
def build_pipeline(X, y):
    model, scaler, X_test_scaled, y_test = helper.train_model(X, y)
    return model, scaler, X_test_scaled, y_test

try:
    with st.spinner("Loading data and training model..."):
        df_raw, X, y, target_col = fetch_and_clean_data()
        model, scaler, X_test_scaled, y_test = build_pipeline(X, y)
except Exception as e:
    st.error(f"Error loading dataset: {e}. Please ensure 'phising-data.csv' is in the same directory.")
    st.stop()

st.subheader("1. Model Evaluation Metrics")
metrics = helper.get_metrics(model, X_test_scaled, y_test)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Accuracy", f"{metrics['Accuracy']:.4f}")
col2.metric("Precision", f"{metrics['Precision']:.4f}")
col3.metric("Recall", f"{metrics['Recall']:.4f}")
col4.metric("F1 Score", f"{metrics['F1 Score']:.4f}")

st.markdown("---")

st.subheader("2. Model Visualizations")
col1, col2 = st.columns(2)

with col1:
    st.write("**Confusion Matrix**")
    fig1, ax1 = plt.subplots(figsize=(5, 4))
    ConfusionMatrixDisplay.from_estimator(model, X_test_scaled, y_test, ax=ax1, cmap='Blues', colorbar=False)
    ax1.grid(False)
    st.pyplot(fig1)

with col2:
    st.write("**ROC-AUC Curve**")
    probs = model.predict_proba(X_test_scaled)[:, 1]
    fpr, tpr, _ = roc_curve(y_test, probs)
    auc_score = roc_auc_score(y_test, probs)
    
    fig2, ax2 = plt.subplots(figsize=(5, 4))
    ax2.plot(fpr, tpr, label=f'AUC = {auc_score:.4f}', color='darkorange', linewidth=2)
    ax2.plot([0, 1], [0, 1], 'k--', linewidth=2, label='Random Chance')
    ax2.set_xlabel('False Positive Rate')
    ax2.set_ylabel('True Positive Rate')
    ax2.legend(loc='lower right')
    st.pyplot(fig2)