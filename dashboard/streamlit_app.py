import os
import sys
import time
import random
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

# Adjust Python path to allow root imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.detector import detect

# Set Streamlit Page Config
st.set_page_config(
    page_title="Real-Time Server Anomaly Monitor",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Premium Styling (CSS)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Space+Grotesk:wght@400;700&display=swap');
    
    /* Global style overrides */
    .stApp {
        background-color: #0b0f19;
        font-family: 'Space Grotesk', sans-serif;
    }
    
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Outfit', sans-serif;
        color: #ffffff;
    }

    /* Main dashboard title styling */
    .dashboard-title {
        font-size: 2.8rem;
        font-weight: 800;
        background: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.1rem;
        padding-top: 1rem;
    }
    
    .dashboard-subtitle {
        text-align: center;
        font-size: 1.1rem;
        color: #8b9bb4;
        margin-bottom: 2rem;
        font-weight: 300;
    }

    /* Status Bar styling */
    .status-container {
        padding: 1.25rem;
        border-radius: 12px;
        text-align: center;
        font-weight: bold;
        font-size: 1.3rem;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        border: 1px solid;
    }
    
    .status-healthy {
        background: rgba(16, 185, 129, 0.12);
        color: #10b981;
        border-color: rgba(16, 185, 129, 0.3);
        box-shadow: 0 0 15px rgba(16, 185, 129, 0.1);
    }
    
    .status-anomalous {
        background: rgba(239, 68, 68, 0.15);
        color: #ef4444;
        border-color: rgba(239, 68, 68, 0.4);
        box-shadow: 0 0 20px rgba(239, 68, 68, 0.25);
        animation: pulse 2.0s infinite alternate;
    }
    
    @keyframes pulse {
        0% { transform: scale(1.0); }
        100% { transform: scale(1.01); box-shadow: 0 0 25px rgba(239, 68, 68, 0.4); }
    }

    /* Glassmorphism Metric cards */
    .glass-card {
        background: rgba(21, 28, 45, 0.7);
        border-radius: 14px;
        padding: 1.5rem;
        border: 1px solid rgba(255, 255, 255, 0.05);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
        backdrop-filter: blur(10px);
        transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
        margin-bottom: 1rem;
    }
    
    .glass-card:hover {
        transform: translateY(-4px);
        border-color: rgba(0, 242, 254, 0.3);
        box-shadow: 0 12px 35px rgba(0, 242, 254, 0.12);
    }
    
    .card-metric-label {
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #7d8da4;
        margin-bottom: 0.5rem;
    }
    
    .card-metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #ffffff;
        font-family: 'Space Grotesk', sans-serif;
    }
    
    .card-metric-unit {
        font-size: 1.1rem;
        color: #7d8da4;
        margin-left: 0.2rem;
    }
    
    /* Hide default Streamlit elements for a cleaner look */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Initialize session state for metrics history
if 'metrics_history' not in st.session_state:
    st.session_state.metrics_history = []
    st.session_state.anomalies_log = []
    st.session_state.iterations_to_anomaly = random.randint(15, 30)

def simulate_metrics(inject_anomaly=False):
    """
    Generates simulated server metrics.
    If inject_anomaly is True, injects high-impact outliers (spikes).
    """
    timestamp = datetime.now().isoformat()

    if not inject_anomaly:
        # ---- Normal healthy behavior ----
        cpu_usage = float(np.clip(np.random.normal(loc=40.0, scale=10.0), 0.0, 100.0))
        memory_usage = float(np.clip(np.random.normal(loc=50.0, scale=8.0), 0.0, 100.0))
        request_latency = float(np.clip(np.random.lognormal(mean=4.8, sigma=0.25), 10.0, 1000.0))
        error_rate = float(np.clip(np.random.beta(a=1, b=20) * 0.1, 0.0, 1.0))
    else:
        # ---- Anomalous behavior ----
        anomaly_type = random.choice(["cpu_spike", "memory_leak", "latency_storm", "error_burst"])

        if anomaly_type == "cpu_spike":
            cpu_usage = float(random.uniform(90.0, 100.0))
            memory_usage = float(random.uniform(60.0, 80.0))
            request_latency = float(random.uniform(500.0, 800.0))
            error_rate = float(random.uniform(0.05, 0.15))
            
        elif anomaly_type == "memory_leak":
            cpu_usage = float(random.uniform(50.0, 75.0))
            memory_usage = float(random.uniform(95.0, 100.0))
            request_latency = float(random.uniform(200.0, 400.0))
            error_rate = float(random.uniform(0.01, 0.05))
            
        elif anomaly_type == "latency_storm":
            cpu_usage = float(random.uniform(30.0, 50.0))
            memory_usage = float(random.uniform(45.0, 60.0))
            request_latency = float(random.uniform(1200.0, 2000.0))
            error_rate = float(random.uniform(0.10, 0.30))
            
        else:  # "error_burst"
            cpu_usage = float(random.uniform(30.0, 50.0))
            memory_usage = float(random.uniform(40.0, 60.0))
            request_latency = float(random.uniform(150.0, 300.0))
            error_rate = float(random.uniform(0.70, 0.98))

    return {
        "timestamp": timestamp,
        "cpu_usage": round(cpu_usage, 2),
        "memory_usage": round(memory_usage, 2),
        "request_latency": round(request_latency, 2),
        "error_rate": round(error_rate, 4)
    }

# Sidebar Configuration
st.sidebar.markdown("""
<div style="text-align: center; margin-bottom: 1.5rem;">
    <h2 style="font-size: 1.6rem; font-weight: 800; background: linear-gradient(135deg, #00f2fe, #4facfe); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">CONTROL PANEL</h2>
</div>
""", unsafe_allow_html=True)

refresh_interval = st.sidebar.slider("Auto-refresh interval (seconds)", min_value=1, max_value=5, value=2)
max_points = st.sidebar.slider("Timeline History Count", min_value=20, max_value=100, value=50)
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("""
### System Details
- **Engine**: Isolation Forest Model
- **Data Source**: Synthetic Real-Time Simulation
- **Update Frequency**: Every 2 seconds
- **Model Training**: 1,000 baseline samples
- **Contamination**: 5% threshold
""")

st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📌 About This Project
- Real-time anomaly detection for server metrics
- Isolation Forest for unsupervised outlier detection
- Dashboard built with Streamlit & Plotly
- [GitHub Repo](https://github.com) | Resume Demo
""")

# Render Main Titles
st.markdown("<div class='dashboard-title'>SERVER ANOMALY MONITOR</div>", unsafe_allow_html=True)
st.markdown("<div class='dashboard-subtitle'>Real-time machine learning infrastructure anomaly detection</div>", unsafe_allow_html=True)

# Generate new metric every iteration
st.session_state.iterations_to_anomaly -= 1
inject = (st.session_state.iterations_to_anomaly <= 0)

payload = simulate_metrics(inject_anomaly=inject)

# Run model scoring
try:
    is_anomaly, anomaly_score = detect(
        payload['cpu_usage'],
        payload['memory_usage'],
        payload['request_latency'],
        payload['error_rate']
    )
except Exception as e:
    st.error(f"Model error: {e}")
    is_anomaly = False
    anomaly_score = 0.0

# Store in history
metric_record = {
    'timestamp': payload['timestamp'],
    'cpu': payload['cpu_usage'],
    'memory': payload['memory_usage'],
    'latency': payload['request_latency'],
    'error_rate': payload['error_rate'],
    'is_anomaly': 1 if is_anomaly else 0,
    'anomaly_score': anomaly_score
}
st.session_state.metrics_history.append(metric_record)

# Keep only max_points in history
if len(st.session_state.metrics_history) > max_points:
    st.session_state.metrics_history = st.session_state.metrics_history[-max_points:]

# Log anomalies
if is_anomaly:
    st.session_state.anomalies_log.append(metric_record)
    if len(st.session_state.anomalies_log) > 50:  # Keep last 50 anomalies
        st.session_state.anomalies_log = st.session_state.anomalies_log[-50:]
    
    # Reset anomaly timer
    st.session_state.iterations_to_anomaly = random.randint(15, 30)

# Render System Status Indicator
latest_point = st.session_state.metrics_history[-1] if st.session_state.metrics_history else metric_record
is_latest_anomalous = latest_point['is_anomaly'] == 1

if is_latest_anomalous:
    status_html = """
    <div class="status-container status-anomalous">
        🚨 CRITICAL ANOMALY DETECTED: System metrics are deviating significantly from model baseline!
    </div>
    """
else:
    status_html = """
    <div class="status-container status-healthy">
        ✅ SYSTEM STATUS: HEALTHY (Operational baselines within normal threshold limits)
    </div>
    """
st.markdown(status_html, unsafe_allow_html=True)

# KPI Cards layout
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="glass-card">
        <div class="card-metric-label">CPU USAGE</div>
        <div class="card-metric-value">{latest_point['cpu']}<span class="card-metric-unit">%</span></div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="glass-card">
        <div class="card-metric-label">MEMORY USAGE</div>
        <div class="card-metric-value">{latest_point['memory']}<span class="card-metric-unit">%</span></div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="glass-card">
        <div class="card-metric-label">LATENCY</div>
        <div class="card-metric-value">{latest_point['latency']:.1f}<span class="card-metric-unit">ms</span></div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="glass-card">
        <div class="card-metric-label">ERROR RATE</div>
        <div class="card-metric-value">{latest_point['error_rate']*100:.2f}<span class="card-metric-unit">%</span></div>
    </div>
    """, unsafe_allow_html=True)

# Plots Column Split
plot_col1, plot_col2 = st.columns(2)

# Convert history to DataFrame for plotting
if st.session_state.metrics_history:
    df = pd.DataFrame(st.session_state.metrics_history)
    df['timestamp_parsed'] = pd.to_datetime(df['timestamp'])

    # Chart 1: CPU & Memory Line Charts
    with plot_col1:
        st.markdown("### CPU & Memory Resource Utilization Timeline")
        
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=df['timestamp_parsed'], 
            y=df['cpu'], 
            name="CPU (%)", 
            line=dict(color="#00f2fe", width=2.5)
        ))
        fig1.add_trace(go.Scatter(
            x=df['timestamp_parsed'], 
            y=df['memory'], 
            name="Memory (%)", 
            line=dict(color="#d946ef", width=2.5)
        ))
        
        fig1.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)", range=[0, 105]),
            height=320
        )
        st.plotly_chart(fig1, use_container_width=True)

    # Chart 2: Latency Scatter with Anomaly Highlights
    with plot_col2:
        st.markdown("### Request Latency Stream & Model Classifications")
        
        fig2 = go.Figure()
        
        normal_df = df[df['is_anomaly'] == 0]
        if not normal_df.empty:
            fig2.add_trace(go.Scatter(
                x=normal_df['timestamp_parsed'],
                y=normal_df['latency'],
                mode='lines+markers',
                name='Normal Latency',
                line=dict(color='rgba(0, 242, 254, 0.4)', width=1.5),
                marker=dict(color='#0ea5e9', size=6)
            ))
        
        anomaly_df = df[df['is_anomaly'] == 1]
        if not anomaly_df.empty:
            fig2.add_trace(go.Scatter(
                x=anomaly_df['timestamp_parsed'],
                y=anomaly_df['latency'],
                mode='markers',
                name='Anomaly Alert',
                marker=dict(
                    color='#ef4444', 
                    size=12, 
                    symbol='triangle-up',
                    line=dict(color='#ffffff', width=1.5)
                )
            ))
            
        fig2.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
            height=320
        )
        st.plotly_chart(fig2, use_container_width=True)

# Lower Section: Anomaly Alert Log
st.markdown("### 🚨 Logged Outlier Alerts")
if st.session_state.anomalies_log:
    log_df = pd.DataFrame(st.session_state.anomalies_log)
    
    display_df = log_df[['timestamp', 'cpu', 'memory', 'latency', 'error_rate', 'anomaly_score']].copy()
    display_df.columns = ['Timestamp', 'CPU Usage (%)', 'Memory Usage (%)', 'Latency (ms)', 'Error Rate (%)', 'Anomaly Score']
    display_df['Error Rate (%)'] = display_df['Error Rate (%)'] * 100
    display_df['Anomaly Score'] = display_df['Anomaly Score'].round(4)
    
    st.dataframe(
        display_df.head(20),
        use_container_width=True
    )
else:
    st.info("No anomalies detected yet. The infrastructure is performing within normal bounds.")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
