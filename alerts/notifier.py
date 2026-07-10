import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from plyer import notification

# Load environment variables from .env file
load_dotenv()

# Retrieve alert settings from configuration
ALERT_METHOD = os.getenv("ALERT_METHOD", "desktop").lower()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL = os.getenv("SMTP_EMAIL", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_RECEIVER = os.getenv("ALERT_RECEIVER", "")

def send_desktop_alert(metric_summary, score):
    """
    Triggers a native desktop notification toast.
    """
    try:
        # Title of the desktop notification
        title = "⚠️ Anomaly Detected! ⚠️"
        
        # Message content detailing the metrics and anomaly score
        message = f"Score: {score:.3f}\nMetrics: {metric_summary}"
        
        # Trigger native OS notification using plyer
        notification.notify(
            title=title,
            message=message,
            app_name="Anomaly Detection System",
            timeout=5  # Duration in seconds the notification displays
        )
        print(f"[Alert System] Desktop notification sent successfully: {metric_summary}")
    except Exception as e:
        print(f"[Alert System] Error sending desktop notification: {e}")

def send_email_alert(metric_summary, score):
    """
    Sends an email alert using SMTP and smtplib.
    Handles authentication and connection security.
    """
    # Defensive checks to ensure email settings are configured
    if not SMTP_EMAIL or SMTP_EMAIL == "your_email@gmail.com" or not SMTP_PASSWORD:
        print("[Alert System] WARNING: SMTP credentials are not configured in .env. Skipping email.")
        return False

    try:
        # Construct the email metadata
        msg = MIMEMultipart()
        msg['From'] = SMTP_EMAIL
        msg['To'] = ALERT_RECEIVER
        msg['Subject'] = "🚨 CRITICAL: Server Anomaly Detected!"

        # Construct the HTML/Plain Text body of the email
        body = f"""
        <h2>Server Anomaly Alert</h2>
        <p>A real-time anomaly was detected in your system metrics.</p>
        <ul>
            <li><strong>Anomaly Score:</strong> {score:.4f} (Severity: High)</li>
            <li><strong>Metrics Details:</strong> {metric_summary}</li>
        </ul>
        <p>Please check your Streamlit monitoring dashboard immediately.</p>
        """
        msg.attach(MIMEText(body, 'html'))

        # Connect to the SMTP server (StartTLS mode for security)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        
        # Authenticate and send email
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.sendmail(SMTP_EMAIL, ALERT_RECEIVER, msg.as_string())
        server.quit()
        
        print(f"[Alert System] Email notification sent successfully to {ALERT_RECEIVER}")
        return True
    except Exception as e:
        print(f"[Alert System] Error sending email notification: {e}")
        return False

def trigger_alert(metric_summary, score):
    """
    Orchestrates anomaly alerts based on configuration in .env.
    Can trigger desktop toasts, SMTP emails, or both.
    """
    print(f"[Alert System] Processing alert for score {score:.4f}...")
    
    if ALERT_METHOD in ("desktop", "both"):
        send_desktop_alert(metric_summary, score)
        
    if ALERT_METHOD in ("email", "both"):
        send_email_alert(metric_summary, score)

# Optional testing script to run this module independently
if __name__ == "__main__":
    print("Testing alerts/notifier.py independently...")
    test_metrics = "CPU: 96.5%, Memory: 92.0%, Latency: 1450ms, Error Rate: 10.0%"
    test_score = 0.354
    
    print(f"Triggering test alert (Method: {ALERT_METHOD})...")
    trigger_alert(test_metrics, test_score)
