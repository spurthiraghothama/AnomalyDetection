import os
import joblib
import pandas as pd
import threading

# Define paths relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, 'anomaly_model.pkl')

# Global variable to cache the loaded model (thread-safe via lock)
_model = None
_model_lock = threading.Lock()

def get_model():
    """
    Returns the loaded Isolation Forest model, loading it from disk on the first call.
    This pattern ensures the model is cached and not repeatedly loaded.
    Thread-safe via a lock for multi-threaded scorer services.
    """
    global _model
    with _model_lock:
        if _model is None:
            if not os.path.exists(MODEL_PATH):
                raise FileNotFoundError(
                    f"Model file not found at: {MODEL_PATH}. "
                    "Please run 'python model/train.py' first to train and save the model."
                )
            # Load the serialized Isolation Forest model
            _model = joblib.load(MODEL_PATH)
        return _model

def reload_model(path=None):
    """
    Hot-reloads the model from disk. Used by the scorer service when a 
    new model checkpoint is downloaded from MinIO.
    
    Parameters:
    - path: Path to the model file to load. Defaults to MODEL_PATH.
    
    Returns:
    - The newly loaded model instance
    """
    global _model
    load_path = path or MODEL_PATH
    if not os.path.exists(load_path):
        raise FileNotFoundError(f"Model file not found at: {load_path}")
    
    new_model = joblib.load(load_path)
    with _model_lock:
        _model = new_model
    print(f"[Detector] Model hot-reloaded from: {load_path}")
    return _model

def set_model(model_instance):
    """
    Directly sets the in-memory model instance.
    Used by the scorer service after retraining without writing to disk first.
    
    Parameters:
    - model_instance: A trained sklearn model instance
    """
    global _model
    with _model_lock:
        _model = model_instance
    print("[Detector] Model instance updated in memory.")

def detect(cpu, memory, latency, error_rate):
    """
    Scores an incoming server metric data point and classifies it.
    
    Parameters:
    - cpu: float (0.0 to 100.0)
    - memory: float (0.0 to 100.0)
    - latency: float (ms)
    - error_rate: float (0.0 to 1.0)
    
    Returns:
    - is_anomaly: bool (True if anomalous, False if normal)
    - anomaly_score: float (higher means more anomalous, generally > 0 is anomalous)
    """
    # 1. Load the model
    model = get_model()

    # 2. Format input as a DataFrame matching the training feature names
    input_data = pd.DataFrame([{
        'cpu_usage': float(cpu),
        'memory_usage': float(memory),
        'request_latency': float(latency),
        'error_rate': float(error_rate)
    }])

    # 3. Get prediction
    # predict() returns 1 for inliers (normal) and -1 for outliers (anomalies)
    prediction = model.predict(input_data)[0]
    is_anomaly = (prediction == -1)

    # 4. Get raw decision function score
    # decision_function() returns values where negative is anomalous and positive is normal.
    # By taking the negative (-decision_function), we get:
    # - Positive scores (e.g. > 0) for anomalies (higher = more anomalous)
    # - Negative scores (e.g. < 0) for normal data points
    raw_decision = model.decision_function(input_data)[0]
    anomaly_score = -raw_decision

    return bool(is_anomaly), float(anomaly_score)

# Optional testing block to verify the detector works independently
if __name__ == "__main__":
    print("Testing detector.py with sample metrics...")
    try:
        # Test normal point
        normal_is_anomaly, normal_score = detect(40.0, 50.0, 120.0, 0.01)
        print(f"Normal Point (40% CPU, 50% Mem, 120ms Latency, 1% Error) -> "
              f"Is Anomaly: {normal_is_anomaly}, Score: {normal_score:.4f}")

        # Test extreme anomaly point
        anomaly_is_anomaly, anomaly_score = detect(98.0, 95.0, 1500.0, 0.8)
        print(f"Anomaly Point (98% CPU, 95% Mem, 1500ms Latency, 80% Error) -> "
              f"Is Anomaly: {anomaly_is_anomaly}, Score: {anomaly_score:.4f}")
              
    except FileNotFoundError as e:
        print(f"Error: {e}")
