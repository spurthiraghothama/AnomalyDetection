import os
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

# Ensure the model directory exists
os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)

def generate_normal_data(num_samples=1000, seed=None):
    """
    Generates synthetic 'normal' operational metrics.
    In real life, you would gather these metrics from production servers during healthy periods.

    Parameters:
    - seed: Fixes the RNG for a reproducible dataset. Leave as None to draw a
      fresh sample. The periodic retrain on the leader node relies on this
      default: seeding here unconditionally made every retrain produce an
      identical model, which defeated the point of retraining at all.
    """
    rng = np.random.default_rng(seed)

    # 1. CPU Usage (%): centered around 40% with a standard deviation of 10%
    cpu = rng.normal(loc=40.0, scale=10.0, size=num_samples)
    cpu = np.clip(cpu, 0.0, 100.0)  # Clip to ensure valid percentage limits [0, 100]

    # 2. Memory Usage (%): centered around 50% with standard deviation of 8%
    memory = rng.normal(loc=50.0, scale=8.0, size=num_samples)
    memory = np.clip(memory, 0.0, 100.0)

    # 3. Request Latency (ms): log-normal distribution to represent typical long-tail response times
    # Exp of normal(4.8, 0.25) centers latency around 120ms with minor fluctuations
    latency = rng.lognormal(mean=4.8, sigma=0.25, size=num_samples)
    latency = np.clip(latency, 20.0, 1000.0)

    # 4. Error Rate (0.0 to 1.0): beta distribution skewed heavily towards 0 (representing low errors)
    # A beta distribution with alpha=1, beta=20 keeps error rate mostly below 5%
    error_rate = rng.beta(a=1, b=20, size=num_samples) * 0.1
    error_rate = np.clip(error_rate, 0.0, 1.0)

    # Combine into a pandas DataFrame for structured representation
    data = pd.DataFrame({
        'cpu_usage': cpu,
        'memory_usage': memory,
        'request_latency': latency,
        'error_rate': error_rate
    })
    
    return data

def train_model(num_samples=1000, save_path=None, seed=None):
    """
    Trains an Isolation Forest model on synthetic normal data and returns it.
    Optionally saves the model to save_path.

    Parameters:
    - num_samples: Number of training samples to generate
    - save_path: If provided, saves the trained model to this path
    - seed: Fixes both the training sample and the forest's splits. Pass an
      int for a reproducible build (used for the image's baked-in model);
      leave as None so each periodic retrain sees a fresh sample.

    Returns:
    - model: The trained IsolationForest instance
    """
    print(f"Generating synthetic operational data ({num_samples} samples)...")
    normal_df = generate_normal_data(num_samples, seed=seed)

    print("\nSample of generated training data:")
    print(normal_df.head())

    print("\nData distribution statistics:")
    print(normal_df.describe())

    # Isolation Forest parameters:
    # - n_estimators: number of decision trees in the forest (100 is standard)
    # - contamination: sets the decision threshold so ~5% of the training set
    #   falls on the anomaly side. It does not change what the forest learns.
    # - random_state: follows `seed`, so a seeded call is fully reproducible
    #   and an unseeded retrain gets different splits as well as different data.
    print("\nInitializing Isolation Forest model...")
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=seed,
    )

    print("Fitting model to normal data...")
    # Train the model using the 4 metrics
    model.fit(normal_df)

    if save_path:
        os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)
        print(f"Saving trained model to: {save_path}")
        joblib.dump(model, save_path)

    print("Model training completed successfully!")
    return model

def train_and_save_model():
    """
    Legacy entry point: trains and saves to the default model path.
    """
    model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anomaly_model.pkl')
    # Seeded: the model baked into the Docker image should be identical on
    # every build. Retrains at runtime deliberately pass no seed.
    train_model(num_samples=1000, save_path=model_path, seed=42)
    print(f"Model serialized to: {model_path}")

if __name__ == "__main__":
    train_and_save_model()
