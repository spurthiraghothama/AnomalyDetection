# Real-Time Anomaly Detection System

A production-grade machine learning system for detecting anomalies in server metrics using real-time streaming and the **Isolation Forest** algorithm.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-Latest-green)
![Kafka](https://img.shields.io/badge/Kafka-7.3.0-red)
![License](https://img.shields.io/badge/License-MIT-yellow)

## 🎯 Live Demo

**View the live demo here**: [https://anomaly-detection-demo.streamlit.app](https://anomaly-detection-demo.streamlit.app)

*(Replace with your deployed URL after following the deployment guide)*

---

## 📋 Features

- ⚡ **Real-Time Anomaly Detection**: Processes metrics in real-time using Isolation Forest
- 📊 **Interactive Dashboard**: Built with Streamlit & Plotly for beautiful visualizations
- 🏗️ **Scalable Architecture**: Kafka-based streaming pipeline decoupled from processing
- 🛠️ **Production-Ready**: Error handling, logging, and graceful degradation
- 📈 **Historical Tracking**: SQLite database stores all metrics and anomalies
- 🔔 **Multi-Channel Alerts**: Desktop notifications and email alerts
- 🤖 **Unsupervised Learning**: Detects anomalies without labeled training data

---

## 🚀 Quick Start (Streamlit Cloud)

Deploy to Streamlit Cloud in **5 minutes**:

1. **Train the model locally**:
   ```bash
   python model/train.py
   ```

2. **Push to GitHub**:
   ```bash
   git add .
   git commit -m "Add anomaly detection system"
   git push origin main
   ```

3. **Deploy to Streamlit Cloud**:
   - Go to https://share.streamlit.io
   - Select your repository
   - Set main file to `dashboard/streamlit_app.py`
   - Click "Deploy"

📖 **Full deployment guide**: See [DEPLOYMENT.md](DEPLOYMENT.md)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Real-Time Pipeline                      │
└─────────────────────────────────────────────────────────────┘

  Producer                 Kafka Broker           Consumer
  (Metrics)          (Topic: server-metrics)     (Orchestrator)
     │                        │                        │
     │──JSON Events──────────►│                        │
     │                        │                        │
     │                        │──Pull Messages────────►│
     │                        │                        │
     │                        │                   ┌────┴─────┐
     │                        │                   │           │
     │                        │            ┌──────▼──┐   ┌────▼────┐
     │                        │            │  Model  │   │ Database │
     │                        │            │(Detector)   │ (SQLite) │
     │                        │            └─────────┘   └──────────┘
     │                        │                   │
     │                        │                   │
     │                        │            ┌──────▼────────┐
     │                        │            │ Alerts Module │
     │                        │            │ (Email/Toast) │
     │                        │            └───────────────┘
     │                        │
     └────────────────────────────────────────────────────────┘

                        Dashboard (Streamlit)
                    Queries SQLite every 3 seconds
                    Renders live Plotly charts
```

### Component Overview

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **Producer** | Simulates server metrics | Python, NumPy |
| **Kafka** | Distributed streaming platform | Apache Kafka, Zookeeper |
| **Consumer** | Orchestrates pipeline | Python, Kafka Python |
| **Model** | Detects anomalies | Scikit-learn (Isolation Forest) |
| **Database** | Persists results | SQLite |
| **Dashboard** | Visualizes metrics | Streamlit, Plotly |
| **Alerts** | Sends notifications | Plyer, SMTP |

---

## 📊 The Isolation Forest Algorithm

**Why Isolation Forest?**

- ✅ **Unsupervised**: No labeled data required
- ✅ **Fast**: O(log n) average complexity
- ✅ **Effective**: Handles multi-dimensional outliers well
- ✅ **Interpretable**: Easy to understand tree structure

**How it works**:
1. Randomly selects features and split points
2. Builds isolation trees to separate anomalies from normal points
3. Anomalies require fewer splits to isolate (shallow trees)
4. Normal points require many splits (deep trees)

---

## 🛠️ Local Development Setup

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Git

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/AnomalyDetection.git
   cd AnomalyDetection
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Train the ML model**:
   ```bash
   python model/train.py
   ```

### Running Locally (Full Stack)

1. **Start Kafka & Zookeeper**:
   ```bash
   docker-compose up -d
   ```

2. **Start the consumer**:
   ```bash
   python consumer.py
   ```

3. **Start the producer** (in another terminal):
   ```bash
   python producer.py
   ```

4. **Launch the dashboard** (in another terminal):
   ```bash
   streamlit run dashboard/app.py
   ```

The dashboard opens at: `http://localhost:8501`

---

## 📁 Project Structure

```
AnomalyDetection/
├── dashboard/
│   ├── app.py                 # Original Kafka-based dashboard
│   └── streamlit_app.py       # Cloud-ready Streamlit version
├── model/
│   ├── train.py              # Train Isolation Forest model
│   ├── detector.py           # Model inference interface
│   └── anomaly_model.pkl     # Trained model (serialized)
├── database/
│   └── db.py                 # SQLite database layer
├── alerts/
│   └── notifier.py           # Email & desktop alerts
├── producer.py               # Metric simulation & Kafka producer
├── consumer.py               # Kafka consumer & orchestrator
├── docker-compose.yml        # Kafka & Zookeeper setup
├── requirements.txt          # Python dependencies
├── .streamlit/config.toml    # Streamlit configuration
├── DEPLOYMENT.md             # Streamlit Cloud deployment guide
└── README.md                 # This file
```

---

## 🧪 Testing the Model

Test the anomaly detector with sample data:

```bash
python model/detector.py
```

Expected output:
```
Normal Point: Is Anomaly: False, Score: -0.1234
Anomaly Point: Is Anomaly: True, Score: 0.5678
```

---

## 📝 Configuration

Create a `.env` file in the project root:

```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=server-metrics
DB_PATH=database/anomalies.db
ALERT_METHOD=desktop
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_EMAIL=your_email@gmail.com
SMTP_PASSWORD=your_app_password
ALERT_RECEIVER=receiver@example.com
```

---

## 🚀 Deployment Options

| Platform | Setup Time | Cost | Best For |
|----------|-----------|------|----------|
| **Streamlit Cloud** | 5 min | Free | Portfolio & demos |
| **Railway** | 10 min | Free tier | Full Docker stack |
| **AWS EC2** | 20 min | Free (1 yr) | Production |
| **DigitalOcean** | 20 min | $5-6/mo | Simple VPS |

👉 **Recommended for resume**: [Streamlit Cloud Guide](DEPLOYMENT.md)

---

## 💡 Use Cases

- **DevOps Monitoring**: Detect anomalies in CPU, memory, latency
- **Application Performance**: Track error rates and response times
- **Infrastructure Health**: Early warning system for system failures
- **Portfolio Project**: Showcase ML + streaming architecture skills

---

## 🎓 Learning Resources

### Isolation Forest
- [Scikit-learn Documentation](https://scikit-learn.org/stable/modules/ensemble.html#isolation-forest)
- [Original Paper](https://ieeexplore.ieee.org/document/5492016)

### Kafka
- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Kafka for Beginners](https://www.cloudkarafka.com/blog/2016-11-11-kafka-for-beginners.html)

### Streamlit
- [Streamlit Docs](https://docs.streamlit.io/)
- [Streamlit Cloud Deployment](https://docs.streamlit.io/streamlit-cloud/get-started)

---

## 🤝 Contributing

Feel free to fork, modify, and improve! Some ideas:
- Add more metrics (disk I/O, network traffic)
- Implement ARIMA for time-series anomalies
- Add retraining pipeline
- Support for multiple model backends

---

## 📄 License

MIT License - feel free to use this project for your portfolio.

---

## 📧 Questions?

- Check [DEPLOYMENT.md](DEPLOYMENT.md) for deployment help
- Review [architecture_explanation.txt](architecture_explanation.txt) for deep dives
- See [learning_guide.md](learning_guide.md) for interview prep

---

**Built with ❤️ for data engineering + ML portfolios**
