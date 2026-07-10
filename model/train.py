import os
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

# Ensure the model directory exists
os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)

def generate_normal_data(num_samples=1000):
    """
    Generates synthetic 'normal' operational metrics.
    In real life, you would gather these metrics from production servers during healthy periods.
    """
    np.random.seed(42)  # For reproducibility

    # 1. CPU Usage (%): centered around 40% with a standard deviation of 10%
    cpu = np.random.normal(loc=40.0, scale=10.0, size=num_samples)
    cpu = np.clip(cpu, 0.0, 100.0)  # Clip to ensure valid percentage limits [0, 100]

    # 2. Memory Usage (%): centered around 50% with standard deviation of 8%
    memory = np.random.normal(loc=50.0, scale=8.0, size=num_samples)
    memory = np.clip(memory, 0.0, 100.0)

    # 3. Request Latency (ms): log-normal distribution to represent typical long-tail response times
    # Exp of normal(4.8, 0.25) centers latency around 120ms with minor fluctuations
    latency = np.random.lognormal(mean=4.8, sigma=0.25, size=num_samples)
    latency = np.clip(latency, 20.0, 1000.0)

    # 4. Error Rate (0.0 to 1.0): beta distribution skewed heavily towards 0 (representing low errors)
    # A beta distribution with alpha=1, beta=20 keeps error rate mostly below 5%
    error_rate = np.random.beta(a=1, b=20, size=num_samples) * 0.1
    error_rate = np.clip(error_rate, 0.0, 1.0)

    # Combine into a pandas DataFrame for structured representation
    data = pd.DataFrame({
        'cpu_usage': cpu,
        'memory_usage': memory,
        'request_latency': latency,
        'error_rate': error_rate
    })
    
    return data

def train_and_save_model():
    print("Generating synthetic operational data (1000 samples)...")
    normal_df = generate_normal_data(1000)

    print("\nSample of generated training data:")
    print(normal_df.head())

    print("\nData distribution statistics:")
    print(normal_df.describe())

    # Isolation Forest parameters:
    # - n_estimators: number of decision trees in the forest (100 is standard)
    # - contamination: proportion of outliers expected in training data (5%)
    # - random_state: fixed seed to ensure same splits on every run
    print("\nInitializing Isolation Forest model...")
    model = IsolationForest(contamination=0.05, random_state=42)

    print("Fitting model to normal data...")
    # Train the model using the 4 metrics
    model.fit(normal_df)

    # Save the trained model to a serialized pickle file using joblib
    model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anomaly_model.pkl')
    print(f"Saving trained model to: {model_path}")
    joblib.dump(model, model_path)
    print("Model training and serialization completed successfully!")

if __name__ == "__main__":
    train_and_save_model()
