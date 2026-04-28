"""
app.py — Axiom Retention Group web application.

Fully Cloud-Native Architecture:
- User credentials and client access rules are pulled dynamically from S3 (config/users.json).
- Client report payloads are pulled from S3.
- Environment variables manage all AWS connections and secrets.
"""

import os
import json
import boto3
import smtplib
from email.message import EmailMessage
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask, render_template, request,
    redirect, url_for, session, abort, flash
)
from werkzeug.security import check_password_hash

# ── Initialization & Config ───────────────────────────────────────────────────

load_dotenv() 

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(32))

# Global Configuration Variables - Edit these when your data updates
TARGET_PAYLOAD_FILE = "results_2026_04_26.json"
USERS_CONFIG_KEY = "config/users.json"

print(f"DEBUG: Initializing app. Target bucket: {os.getenv('S3_BUCKET_NAME')}")


# ── AWS S3 Data Pipelines ─────────────────────────────────────────────────────

def get_json_from_s3(file_key):
    """Generic function to pull and parse any JSON file from the configured S3 bucket."""
    try:
        bucket = os.getenv('S3_BUCKET_NAME')
        if not bucket:
            print("❌ Error: S3_BUCKET_NAME is missing from environment.")
            return None

        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=file_key)
        content = response['Body'].read()
        
        if not content:
            print(f"⚠️ Warning: File {file_key} is empty.")
            return None
            
        return json.loads(content)

    except Exception as e:
        print(f"❌ S3 Failure for {file_key}: {e}")
        return None

def load_user_config():
    """Pulls the latest user and access dictionaries from S3."""
    data = get_json_from_s3(USERS_CONFIG_KEY)
    if data:
        return data.get("users", {}), data.get("client_access", {})
    
    print("CRITICAL: Could not load user configuration from S3.")
    return {}, {}


# ── Auth Decorator ────────────────────────────────────────────────────────────

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
    # if "username" in session:
    #     return redirect(url_for("home"))
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if "username" in session:
            return redirect(url_for("home"))
    
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        # Fetch fresh credentials from S3 on every login attempt
        users_db, access_db = load_user_config()
        user = users_db.get(username)
        
        if user and check_password_hash(user["password_hash"], password):
            session.clear()
            session["username"]   = username
            # session["display_name"] = user.get("display_name")
            session["client_ids"] = access_db.get(username, [])
            session.permanent     = False  

            next_url = request.args.get("next")
            return redirect(next_url if next_url else url_for("home"))

        error = "Invalid credentials."

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

def list_json_keys_in_s3(prefix):
    """Lists all JSON files in a specific S3 'folder' prefix."""
    try:
        s3 = boto3.client('s3')
        bucket = os.getenv('S3_BUCKET_NAME')
        
        # list_objects_v2 is the standard for 'scraping' a folder
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        # If the folder is empty or doesn't exist, return an empty list
        if 'Contents' not in response:
            return []
            
        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    except Exception as e:
        print(f"❌ Error listing S3 files for {prefix}: {e}")
        return []
    
    
@app.route("/home")
@login_required
def home():
    client_ids = session.get("client_ids", [])
    all_reports = [] # This will hold a card for EVERY json file found

    for cid in client_ids:
        # 1. Scrape the folder for all available reports
        folder_prefix = f"{cid}/payloads/"
        report_keys = list_json_keys_in_s3(folder_prefix)

        for key in report_keys:
            # 2. Fetch each one to get its specific metadata
            payload_data = get_json_from_s3(key)
            if payload_data:
                meta = payload_data.get("meta", {})
                
                # We extract the filename from the full S3 key for the URL
                filename = key.split('/')[-1]
                
                all_reports.append({
                    "client_id":     cid,
                    "filename":      filename,
                    "report_name":   meta.get("report_name", cid),
                    "snapshot_date": meta.get("snapshot_date", "—"),
                    "total_members": meta.get("total_members", "—"),
                })

    # Sort reports by date (optional, but professional)
    all_reports.sort(key=lambda x: x['snapshot_date'], reverse=True)
    
    return render_template("home.html", reports=all_reports)

@app.route("/dashboard/<client_id>/<filename>") # Now takes TWO variables
@login_required
def dashboard(client_id, filename):
    if client_id not in session.get("client_ids", []):
        abort(403)

    # Build the path using the specific filename clicked
    payload_key = f"{client_id}/payloads/{filename}"
    data = get_json_from_s3(payload_key)

    if not data:
        abort(404)

    return render_template("dashboard.html", data=data)






@app.route("/request-access", methods=["POST"])
def request_access():
    email_lead = request.form.get("email")
    print(f"Lead captured: {email_lead}")
    
    msg = EmailMessage()
    msg.set_content(f"New Axiom Demo Request from: {email_lead}")
    msg['Subject'] = "🚀 New Lead: Axiom Retention"
    msg['From'] = os.getenv("EMAIL_USER")
    msg['To'] = os.getenv("EMAIL_USER") 

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
            smtp.send_message(msg)
    except Exception as e:
        print(f"Email failed: {e}")

    flash("Someone from our team will be in touch!!")
    return redirect(url_for('index') + '#access')




@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload_data():
    if request.method == "POST":
        client_id = request.form.get("client_id")
        file = request.files.get("file")

        if not file or not client_id:
            flash("Missing file or client selection.")
            return redirect(request.url)

        # Security: Ensure the user is actually allowed to upload for this client
        if client_id not in session.get("client_ids", []):
            abort(403)

        try:
            s3 = boto3.client('s3')
            # Define the 'raw' folder path
            file_key = f"{client_id}/raw/{file.filename}"
            
            s3.upload_fileobj(
                file,
                os.getenv('S3_BUCKET_NAME'),
                file_key
            )
            
            flash(f"Successfully uploaded {file.filename} to {client_id}/raw/")

            # email notification to admin
            msg = EmailMessage()
            msg.set_content(f"New file uploaded for {client_id}: {file.filename}")
            msg['Subject'] = f"📥 New Upload: {client_id}"
            msg['From'] = os.getenv("EMAIL_USER")
            msg['To'] = os.getenv("EMAIL_USER") 
            try:
                with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                    smtp.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
                    smtp.send_message(msg)
            except Exception as e:
                print(f"Email failed: {e}")


            return redirect(url_for('home'))
            
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            flash("Upload failed. Check server logs.")
    
    # GET request: Show the upload form
    return render_template("upload.html", client_ids=session.get("client_ids", []))


@app.context_processor
def inject_user():
    # Priority: 1. username, 2. display_name, 3. Fallback to "User"
    user_identity = session.get("username", session.get("display_name", "User"))

    user_identity = user_identity.capitalize() if isinstance(user_identity, str) else "User"
    return dict(username=user_identity)


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, port=5000)