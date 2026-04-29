"""
app.py — Axiom Retention Group web application.

Fully Cloud-Native Architecture:
- User credentials and client access rules are pulled dynamically from S3 (config/users.json).
- Client report payloads are pulled from S3.
- Environment variables manage all AWS connections and secrets.
"""

import sqlite3
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



import os
import sqlite3

# This finds the directory where app.py lives (3.website)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# This points to the parent directory for the database
# ".." means "go up one level"
DB_PATH = os.path.join(BASE_DIR, "..", "axiom.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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

import sys
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
            session["role"] = user.get("role")
            session["display_name"] = user.get("display_name")
            print(f"✅ Login successful for {username} with role {session['role']}")
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

        try:
            # 1. S3 Upload
            s3 = boto3.client('s3')
            file_key = f"{client_id}/raw/{file.filename}"
            s3.upload_fileobj(file, os.getenv('S3_BUCKET_NAME'), file_key)
            
            # 2. SQL Log - Wrap this specifically to see if DB is the issue
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO uploads (filename, client_name, status) VALUES (?, ?, ?)",
                    (file.filename, client_id, 'PENDING')
                )
                conn.commit()
                conn.close()
            except Exception as db_err:
                print(f"Database Error: {db_err}")
                # We don't want to stop the whole process if just the log fails, 
                # but we need to know!

            # 3. Email Notification
            # Ensure 'from email.message import EmailMessage' is at the top!
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
                print(f"Email notification failed but upload succeeded: {e}")

            flash(f"Successfully uploaded {file.filename}!")
            return redirect(url_for('home')) # This should now trigger
            
        except Exception as e:
            # If we get here, S3 or the core logic failed
            print(f"❌ CRITICAL UPLOAD FAILURE: {e}")
            flash(f"Upload failed: {e}")
            return redirect(request.url) # Redirect back so they can try again
    
    return render_template("upload.html", client_ids=session.get("client_ids", []))

@app.context_processor
def inject_user():
    # Priority: 1. username, 2. display_name, 3. Fallback to "User"
    user_identity = session.get("username", session.get("display_name", "User"))

    user_identity = user_identity.capitalize() if isinstance(user_identity, str) else "User"
    return dict(username=user_identity)


@app.route("/admin")
@login_required
def admin_portal():
    if session.get("role") != "admin":
        abort(403)

    conn = sqlite3.connect(DB_PATH) 
    # This line is CRITICAL. Without it, you can't use upload['filename']
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    
    try:
        uploads = cursor.execute("SELECT * FROM uploads ORDER BY timestamp DESC").fetchall()
    except sqlite3.OperationalError as e:
        # This will tell you if the TABLE doesn't exist yet
        return f"Database Error: {e}" 
    finally:
        conn.close()

    return render_template("admin.html", uploads=uploads)




@app.route("/admin/update/<int:file_id>/<new_status>")
@login_required
def change_status(file_id, new_status):
    if session.get("role") != "admin":
        abort(403)

    conn = sqlite3.connect('axiom.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE uploads SET status = ? WHERE id = ?", (new_status.upper(), file_id))
    conn.commit()
    conn.close()

    return redirect(url_for('admin_portal'))
# ── Run ───────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     app.run(debug=True, port=5000)

if __name__ == "__main__":
    # host="0.0.0.0" tells Flask to listen on all public IPs
    app.run(host="0.0.0.0", port=80)