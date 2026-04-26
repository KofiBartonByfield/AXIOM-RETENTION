"""
app.py — Axiom Retention Group web application.

Routes:
    GET  /              → redirect to /home if logged in, else /login
    GET  /login         → login page
    POST /login         → authenticate and redirect to /home
    GET  /logout        → clear session, redirect to /login
    GET  /home          → client selection landing page (login required)
    GET  /dashboard/<client_id> → report dashboard for a specific client

Client data lives in data/<client_id>/report_payload.json.
Users are defined in users.py — never in this file.
"""

import json
import os
from functools import wraps
from pathlib import Path

from flask import (
    Flask, render_template, request,
    redirect, url_for, session, abort,
)
from werkzeug.security import check_password_hash

from users import USERS, CLIENT_ACCESS

app = Flask(__name__)

# SECRET_KEY must be set via environment variable in production.
# Never hardcode this. Generate with: python -c "import os; print(os.urandom(32).hex())"
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))


# Pull from .env
raw_path = os.getenv("client_data")

# Convert the string into a Path object
if raw_path:
    DATA_DIR = Path(raw_path) 
else:
    DATA_DIR = Path(__file__).parent / "data"



# ── Auth decorator ────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "username" not in session:
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    # 1. If they have the 'seal' (cookie), send them to the reports
    if "username" in session:
        return redirect(url_for("home"))
    
    # 2. If no cookie, show them the landing page (with your login button)
    return render_template("index.html")
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = USERS.get(username)
        if user and check_password_hash(user["password_hash"], password):
            session.clear()
            session["username"]    = username
            session["client_ids"]  = CLIENT_ACCESS.get(username, [])
            session.permanent      = False   # session expires on browser close

            next_url = request.args.get("next")
            return redirect(next_url if next_url else url_for("home"))

        error = "Invalid credentials."

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    username   = session["username"]
    client_ids = session.get("client_ids", [])

    # Build client cards from their payload metadata
    clients = []
    for cid in client_ids:
        payload_path = DATA_DIR / cid / "1. Outputs" / "report_payload.json"

        if payload_path.exists():
            with open(payload_path) as f:
                meta = json.load(f).get("meta", {})
            clients.append({
                "id":            cid,
                "name":          meta.get("report_name", cid),
                "snapshot_date": meta.get("snapshot_date", "—"),
                "total_members": meta.get("total_members", "—"),
            })

    return render_template("home.html", username=username, clients=clients)


@app.route("/dashboard/<client_id>")
@login_required
def dashboard(client_id):
    # Authorisation check — user may only view their permitted clients
    if client_id not in session.get("client_ids", []):
        abort(403)

    payload_path = DATA_DIR / client_id / "1. Outputs" / "report_payload.json"
    if not payload_path.exists():
        abort(404)

    with open(payload_path) as f:
        data = json.load(f)

    return render_template("dashboard.html", data=data)


from flask import flash # Add this to your imports

# @app.route("/request-access", methods=["POST"])
# def request_access():
#     email = request.form.get("email")
#     print(f"Lead captured: {email}")
    
#     flash("Someone from our team will be in touch!!")
#     return redirect(url_for('index'))


import smtplib
from email.message import EmailMessage

@app.route("/request-access", methods=["POST"])
def request_access():
    email_lead = request.form.get("email")
    
    # 1. Log it locally just in case
    print(f"Lead captured: {email_lead}")
    
    # 2. Send the Notification Email
    msg = EmailMessage()
    msg.set_content(f"New Axiom Demo Request from: {email_lead}")
    msg['Subject'] = "🚀 New Lead: Axiom Retention"
    msg['From'] = os.getenv("EMAIL_USER")
    msg['To'] = os.getenv("EMAIL_USER") # Sending it to yourself

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
            smtp.send_message(msg)
    except Exception as e:
        print(f"Email failed: {e}")

    flash("Someone from our team will be in touch!!")
    return redirect(url_for('index') + '#access')

# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # debug=True only for local development — never in production
    app.run(debug=True, port=5000)
