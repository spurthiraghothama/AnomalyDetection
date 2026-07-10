# Deploying to Streamlit Cloud

This guide walks you through deploying the Real-Time Anomaly Detection System to Streamlit Cloud in 5 minutes.

## Prerequisites

- A GitHub account (free at github.com)
- A Streamlit Cloud account (free at share.streamlit.io)
- The trained model file must be committed to GitHub

## Step 1: Train the Model Locally

Before pushing to GitHub, ensure the trained model exists:

```bash
python model/train.py
```

This creates `model/anomaly_model.pkl` (will be ~50KB).

## Step 2: Create/Initialize Git Repository

If you haven't already:

```bash
cd e:\OneDrive\Desktop\AnomalyDetection
git init
git add .
git commit -m "Initial commit: Anomaly detection system for Streamlit Cloud"
```

## Step 3: Push to GitHub

Create a new repository on GitHub (github.com/new), then:

```bash
git remote add origin https://github.com/YOUR_USERNAME/AnomalyDetection.git
git branch -M main
git push -u origin main
```

**Replace `YOUR_USERNAME` with your actual GitHub username.**

## Step 4: Deploy to Streamlit Cloud

1. Go to https://share.streamlit.io
2. Click **"New app"**
3. Select:
   - **Repository**: `YOUR_USERNAME/AnomalyDetection`
   - **Branch**: `main`
   - **Main file path**: `dashboard/streamlit_app.py`
4. Click **"Deploy!"**

Streamlit will automatically install dependencies from `requirements.txt` and start the app.

**Your app will be live at**: `https://anomaly-detection-YOUR_USERNAME.streamlit.app`

## Step 5: Share Your Resume Link

Add this URL to your resume:

```
Live Demo: https://anomaly-detection-YOUR_USERNAME.streamlit.app
GitHub: https://github.com/YOUR_USERNAME/AnomalyDetection
```

---

## What's Different from Local?

| Aspect | Local (Kafka) | Streamlit Cloud |
|--------|---------------|-----------------|
| Data Source | Kafka topic | In-memory simulation |
| Database | SQLite file | Session state (in-memory) |
| Alerts | Desktop/Email | Dashboard only |
| Model | `model/detector.py` | `model/detector.py` ✅ |
| Retraining | `model/train.py` | Not available on Cloud |

## Troubleshooting

### "ModuleNotFoundError: No module named 'model'"
- Ensure the model file path is correct in `dashboard/streamlit_app.py`
- The app should automatically find it since it's in the repo

### "FileNotFoundError: Model file not found"
- Run `python model/train.py` locally first
- Commit `model/anomaly_model.pkl` to GitHub

### App is slow or timing out
- Streamlit Cloud has a 5-second startup limit per interaction
- The app is optimized but may take a moment to load charts initially

### Model detection not working
- Check that `model/anomaly_model.pkl` exists locally and on GitHub
- Verify `model/detector.py` is in the repo

## Advanced: Custom Domain

To use a custom domain instead of `streamlit.app`:

1. Go to your app settings on Streamlit Cloud
2. Add a custom domain (requires DNS CNAME record)
3. Point your domain's CNAME to `streamlit.app`

## What Interviewers Will See

1. ✅ **Live, working dashboard** with real-time anomaly detection
2. ✅ **Beautiful Plotly visualizations** showing system metrics
3. ✅ **Anomaly log** with detected outliers
4. ✅ **Sleek glassmorphic UI** in dark mode
5. ✅ **Consistent data updates** from the simulation
6. ✅ **Isolation Forest model** running in real-time

## URL to Add to Resume

```
https://anomaly-detection-YOUR_USERNAME.streamlit.app
```

---

**Total Deploy Time: ~5 minutes** ⚡

Need help? See the main [README.md](README.md) for architecture details.
