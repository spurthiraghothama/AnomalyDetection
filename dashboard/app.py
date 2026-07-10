import os
import sys
import time
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

# Adjust Python path to allow root imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_recent_metrics, get_anomalies_only

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

# Sidebar Configuration
st.sidebar.markdown("""
<div style="text-align: center; margin-bottom: 1.5rem;">
    <h2 style="font-size: 1.6rem; font-weight: 800; background: linear-gradient(135deg, #00f2fe, #4facfe); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">CONTROL PANEL</h2>
</div>
""", unsafe_allow_html=True)

refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=3)
max_points = st.sidebar.slider("Timeline History Count", min_value=20, max_value=200, value=50)

st.sidebar.markdown("---")
st.sidebar.markdown("""
### System Details
- **Engine**: Isolation Forest Model
- **Data Source**: Apache Kafka
- **Database**: SQLite3
- **Streaming Interval**: 2.0s
""")

# Render Main Titles
st.markdown("<div class='dashboard-title'>SERVER ANOMALY MONITOR</div>", unsafe_allow_html=True)
st.markdown("<div class='dashboard-subtitle'>Real-time machine learning infrastructure log streaming analytics</div>", unsafe_allow_html=True)

# Fetch data from Database
try:
    recent_data = get_recent_metrics(max_points)
    all_anomalies = get_anomalies_only()
except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Please make sure the consumer daemon has initialized the SQLite database.")
    recent_data = []
    all_anomalies = []

if not recent_data:
    st.warning("No metrics captured yet. Waiting for Kafka consumer to start writing events...")
    time.sleep(refresh_interval)
    st.rerun()

# Convert metrics list to DataFrame
df = pd.DataFrame(recent_data)
# Convert string timestamp to pandas datetime object
df['timestamp_parsed'] = pd.to_datetime(df['timestamp'])

# Get latest recorded data point
latest_point = df.iloc[-1]
is_latest_anomalous = latest_point['is_anomaly'] == 1

# Render System Status Indicator
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

# Chart 1: CPU & Memory Line Charts
with plot_col1:
    st.markdown("### CPU & Memory Resource Utilization Timeline")
    
    fig1 = go.Figure()
    # Add CPU line
    fig1.add_trace(go.Scatter(
        x=df['timestamp_parsed'], 
        y=df['cpu'], 
        name="CPU (%)", 
        line=dict(color="#00f2fe", width=2.5)
    ))
    # Add Memory line
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
    
    # 1. Normal points
    normal_df = df[df['is_anomaly'] == 0]
    fig2.add_trace(go.Scatter(
        x=normal_df['timestamp_parsed'],
        y=normal_df['latency'],
        mode='lines+markers',
        name='Normal Latency',
        line=dict(color='rgba(0, 242, 254, 0.4)', width=1.5),
        marker=dict(color='#0ea5e9', size=6)
    ))
    
    # 2. Anomaly points (crimson glow)
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
if all_anomalies:
    log_df = pd.DataFrame(all_anomalies)
    
    # Clean up column names for visual readability
    display_df = log_df[['timestamp', 'cpu', 'memory', 'latency', 'error_rate', 'anomaly_score']].copy()
    display_df.columns = ['Timestamp', 'CPU Usage (%)', 'Memory Usage (%)', 'Latency (ms)', 'Error Rate (%)', 'Anomaly Score']
    display_df['Error Rate (%)'] = display_df['Error Rate (%)'] * 100
    display_df['Anomaly Score'] = display_df['Anomaly Score'].round(4)
    
    # Style DataFrame display table
    st.dataframe(
        display_df.head(20),
        use_container_width=True
    )
else:
    st.info("No anomalies detected in logging history yet. The infrastructure is performing within normal bounds.")

# Auto refresh block
time.sleep(refresh_interval)
st.rerun()
