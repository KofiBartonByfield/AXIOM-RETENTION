import os
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv



import shap
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report, recall_score
from pathlib import Path
import json
from datetime import datetime



# Load your .env file
load_dotenv()


REQUIRED_COLUMNS = [
    "member_id",
    "tenure_months",
    "avg_monthly_visits",
    "visits_last_30_days",
    "visits_prev_30_days",
    "contract_type",
    "age",
    "membership_fee",
    "velocity",
    "churned",
]

FEATURE_COLUMNS = [
    "tenure_months",
    "avg_monthly_visits",
    "visits_last_30_days",
    "visits_prev_30_days",
    "contract_type",
    "age",
    "membership_fee",
    "velocity",
]







#── Pull AWS Data
def get_data_from_s3(file_key):
    # 1. Initialize the S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='eu-west-2' # Double-check your bucket region!
    )
    
    bucket_name = os.getenv('S3_BUCKET_NAME')

    try:
        # 2. Grab the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # 3. Read the 'Body' (the actual data) into memory
        data = response['Body'].read()
        
        # 4. Convert to a DataFrame
        df = pd.read_csv(BytesIO(data))
        return df

    except Exception as e:
        print(f"Error pulling from S3: {e}")
        return None
def upload_json_to_s3(data_dict, file_name, client="demo_example"):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    # Convert your dictionary to a JSON string
    json_data = json.dumps(data_dict)
    
    try:
        s3.put_object(
            Bucket=os.getenv('S3_BUCKET_NAME'),
            Key=f"{client}/payloads/{file_name}", # Saves in an 'outputs' folder
            Body=json_data,
            ContentType='application/json' # This tells S3 it's a JSON file
        )
        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading JSON: {e}")



#── SHAP driver extraction 
def _human_label(feature: str, shap_value: float) -> str:
    """
    Convert a feature name + SHAP sign into a one-line plain English
    explanation suitable for the watchlist and client report.
    """
    direction = "increasing" if shap_value > 0 else "decreasing"

    labels = {
        "velocity":             f"Visit frequency is {'falling sharply' if shap_value > 0 else 'stable or rising'}",
        "visits_last_30_days":  f"Recent visits are {'very low' if shap_value > 0 else 'healthy'}",
        "avg_monthly_visits":   f"Historically {'low' if shap_value > 0 else 'high'} engagement",
        "contract_type":        f"{'Monthly' if shap_value > 0 else 'Annual'} contract — {'higher' if shap_value > 0 else 'lower'} churn risk",
        "tenure_months":        f"{'New member' if shap_value > 0 else 'Long-standing member'} — {'onboarding risk' if shap_value > 0 else 'lifestyle shift risk'}",
        "membership_fee":       f"{'Higher' if shap_value > 0 else 'Lower'} fee tier",
        "age":                  f"Age profile {'associated with higher' if shap_value > 0 else 'associated with lower'} churn",
        "visits_prev_30_days":  f"Prior period visits were {'low' if shap_value > 0 else 'high'}",
    }

    return labels.get(feature, f"{feature} is {direction} churn risk")

def get_top_drivers(
    model_pipeline: Pipeline,
    X: pd.DataFrame,
    top_n: int = 3,
) -> pd.DataFrame:
    """
    Compute the top N churn drivers for every member using SHAP.
    Dispatches to the correct explainer based on the model type.

    Works for any sklearn Pipeline with a 'scaler' and 'classifier' step.

    Returns a DataFrame with one row per member:
        member_idx | driver_1 | direction_1 | driver_2 | ...
    """
    classifier = model_pipeline.named_steps['classifier']
    scaler     = model_pipeline.named_steps['scaler']

    # Scale the features as the model sees them
    X_scaled = pd.DataFrame(
        scaler.transform(X),
        columns=X.columns,
        index=X.index,
    )

    if isinstance(classifier, XGBClassifier):
        explainer  = shap.TreeExplainer(classifier)
        shap_vals  = explainer.shap_values(X_scaled)

    elif isinstance(classifier, LogisticRegression):
        explainer  = shap.LinearExplainer(
            classifier,
            X_scaled,
            feature_perturbation="correlation_dependent",
        )
        shap_vals  = explainer.shap_values(X_scaled)

    else:
        raise TypeError(f"No SHAP explainer configured for {type(classifier)}")

    # shap_vals shape: (n_members, n_features)
    records = []
    for i in range(len(X_scaled)):
        row      = shap_vals[i]
        top_idx  = np.argsort(np.abs(row))[::-1][:top_n]
        record   = {"member_idx": X.index[i]}

        for rank, idx in enumerate(top_idx, 1):
            val   = float(row[idx])
            fname = X.columns[idx]
            record[f"driver_{rank}"]     = fname
            record[f"driver_{rank}_impact"] = round(val, 4)
            # Plain English direction for the report
            record[f"driver_{rank}_label"] = _human_label(fname, val)

        records.append(record)

    return pd.DataFrame(records)


# ── Risk classification 
def classify_risk(prob: float) -> str:
    if prob > 90:
        return "CRITICAL"
    if prob > 80:
        return "HIGH"
    if prob > 70:
        return "ELEVATED"
    return "MONITOR"

# ── Core pipeline function
def run_pipeline(
 
    watchlist_threshold: float = 70.0,
    assumed_save_rate: float = 0.30,
    long_term_months: int = 12,
    test_size: float = 0.20,
    random_seed: int = 42,
    client: str = "demo_example",
    raw_file: str = "members_26042026.csv",
    # output_path: str = os.getenv('payload'),
) -> dict:
    """
    Full Axiom churn prediction pipeline.

    Parameters
    ----------
    df                   : clean DataFrame matching the standardised schema
    watchlist_threshold  : minimum churn probability (%) to appear on watchlist
    assumed_save_rate    : conservative fraction of watchlist members that can
                           be saved — used for revenue recovery projections
    long_term_months     : months used to project long-term VaR (default 12)
    test_size            : proportion of data held out for evaluation
    random_seed          : reproducibility seed
    output_path          : path for the JSON report payload

    Returns
    -------
    dict — the full report payload (also written to output_path)
    """
    df=get_data_from_s3(f"{client}/processed/{raw_file}")

    # ── Validation ────────────────────────────────────────────────────────────
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Input data is missing required columns: {sorted(missing)}")

    n_members = len(df)
    churn_rate = df['churned'].mean() * 100
    print(f"Pipeline started | {n_members} members | churn rate={churn_rate}%")

    # ── Split ─────────────────────────────────────────────────────────────────
    X = df[FEATURE_COLUMNS]
    y = df["churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, stratify=y, random_state=random_seed
    )

    # ── Model candidates ──────────────────────────────────────────────────────
    # GridSearchCV on recall: missing a churner costs more than a false alarm
    scaler = StandardScaler()

    candidates = [
        (
            "Logistic_Regression",
            LogisticRegression(max_iter=1000),
            {
                "classifier__C":            [0.1, 1.0, 10.0],
                "classifier__class_weight": ["balanced", None],
            },
        ),
        (
            "XGBoost",
            XGBClassifier(eval_metric="logloss", verbosity=0),
            {
                "classifier__n_estimators":    [50, 100],
                "classifier__max_depth":       [3, 5],
                "classifier__learning_rate":   [0.1],
                "classifier__scale_pos_weight": [3, 5],
            },
        ),
    ]

    results = {}
    for name, model, params in candidates:
        pipe = Pipeline([("scaler", scaler), ("classifier", model)])
        grid = GridSearchCV(pipe, params, cv=3, scoring="recall", n_jobs=-1)
        grid.fit(X_train, y_train)

        y_pred = grid.best_estimator_.predict(X_test)
        test_recall = recall_score(y_test, y_pred)

        results[name] = {
            "best_params": grid.best_params_,
            "cv_recall":   round(grid.best_score_, 4),
            "test_recall": round(test_recall, 4),
            "model_obj":   grid.best_estimator_,
            "report":      classification_report(y_test, y_pred, output_dict=True),
        }
        print(f"  {name} | CV recall={grid.best_score_:.3f} | test recall={test_recall:.3f}")

    # ── Winner selection ──────────────────────────────────────────────────────
    winner_name = max(results, key=lambda k: results[k]["test_recall"])
    winner_model = results[winner_name]["model_obj"]
    print(f"Winner: {winner_name}")

    # ── Score all members ─────────────────────────────────────────────────────
    df = df.copy()
    df["churn_probability"] = (winner_model.predict_proba(X)[:, 1] * 100).round(2)

    # ── Watchlist ─────────────────────────────────────────────────────────────
    watchlist_df = df[df["churn_probability"] >= watchlist_threshold].copy()

    watchlist_df["short_term_var"] = (
        (watchlist_df["churn_probability"] / 100) * watchlist_df["membership_fee"]
    ).round(2)

    watchlist_df["long_term_var"] = (
        watchlist_df["short_term_var"] * long_term_months
    ).round(2)

    watchlist_df["risk_level"] = watchlist_df["churn_probability"].apply(classify_risk)

    watchlist_df = watchlist_df[
        ["member_id", "churn_probability", "risk_level",
         "membership_fee", "short_term_var", "long_term_var"]
    ].sort_values("long_term_var", ascending=False)

    # ── Revenue impact projections ────────────────────────────────────────────
    total_members          = n_members
    total_monthly_revenue  = float(df["membership_fee"].sum())
    watchlist_count        = len(watchlist_df)
    total_short_var        = float(watchlist_df["short_term_var"].sum())
    total_long_var         = float(watchlist_df["long_term_var"].sum())
    avg_risk_score         = float(watchlist_df["churn_probability"].mean())

    # Conservative recovery estimate at assumed_save_rate
    saveable_count         = int(watchlist_count * assumed_save_rate)
    # Use average fee of watchlist members for the recovery projection
    avg_watchlist_fee      = float(watchlist_df["membership_fee"].mean()) if watchlist_count > 0 else 0.0
    monthly_recovery       = round(saveable_count * avg_watchlist_fee, 2)
    annual_recovery        = round(monthly_recovery * long_term_months, 2)

    risk_distribution = watchlist_df["risk_level"].value_counts().to_dict()

    # ── Model performance summary ─────────────────────────────────────────────
    winner_report = results[winner_name]["report"]
    model_summary = {
        "winner":      winner_name,
        "best_params": results[winner_name]["best_params"],
        "cv_recall":   results[winner_name]["cv_recall"],
        "test_recall": results[winner_name]["test_recall"],
        "precision":   round(winner_report["1"]["precision"], 4),
        "f1_score":    round(winner_report["1"]["f1-score"], 4),
        "all_models":  {
            k: {"cv_recall": v["cv_recall"], "test_recall": v["test_recall"]}
            for k, v in results.items()
        },
    }

    # ── SHAP drivers ──────────────────────────────────────────────────────────
    drivers_df = get_top_drivers(winner_model, X, top_n=3)

    # Merge drivers onto watchlist by index before serialising
    watchlist_df = watchlist_df.merge(
        drivers_df, left_index=True, right_on="member_idx", how="left"
    ).drop(columns=["member_idx"])


    watchlist_fees  = watchlist_df["membership_fee"]
    avg_fee         = float(df["membership_fee"].mean())
    avg_tenure      = float(df["tenure_months"].mean())
    avg_ltv         = round(avg_fee * avg_tenure, 2)



    # ── JSON report payload ───────────────────────────────────────────────────
    payload = {
        "meta": {
            "generated_at":      datetime.now().isoformat(),
            "pipeline_version":  "1.4.1",
            "client_name":       client,
            "report_name":       f"{datetime.now().strftime('%B %d, %Y')} Churn Report", # change this to 1st Jan 2026 format
            "total_members":     total_members,
            "snapshot_date":     datetime.now().strftime('%d/%m/%Y'),
            "model_type":        winner_name,
            "drivers_computed":  True,
            "top_n_drivers":     3,
        },
        "summary": {
            "description":            "This report identifies members at risk of churning and projects potential revenue recovery from targeted interventions.",
            "churn_rate":            round(churn_rate, 2),
            "total_monthly_revenue":   total_monthly_revenue,
            "watchlist_count":         watchlist_count,
            "watchlist_threshold_pct": watchlist_threshold,
            "avg_risk_score":          round(avg_risk_score, 2),
            "total_short_term_var":    total_short_var,
            "total_long_term_var":     total_long_var,
            "risk_distribution":       risk_distribution,
        },
        "recovery_projection": {
            "assumed_save_rate": assumed_save_rate,
            "saveable_members":  saveable_count,
            "monthly_recovery":  monthly_recovery,
            "annual_recovery":   annual_recovery,
            "long_term_months":  long_term_months,
        },
        "member_ltv": {
            "avg_monthly_fee":    round(avg_fee, 2),
            "avg_tenure_months":  round(avg_tenure, 1),
            "avg_ltv":            avg_ltv,
        },
        
        "model": model_summary,
        "watchlist": watchlist_df.to_dict(orient="records"),
    }

    # ── Write output ──────────────────────────────────────────────────────────
    upload_json_to_s3(payload, f"{client}_results_{datetime.now().strftime('%d_%m_%Y')}.json", client=client)  
    # upload_json_to_s3(payload, "results_2026_04_26.json")
    # out = Path(output_path)
    # out.parent.mkdir(parents=True, exist_ok=True)
    # with open(out, "w") as f:
    #     json.dump(payload, f, indent=2)

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n─── AXIOM AUDIT RESULTS ───────────────────────────────────")
    print(f"  Members analysed       : {total_members}")
    print(f"  On watchlist (≥{watchlist_threshold:.0f}%)   : {watchlist_count}")
    print(f"  Avg risk score         : {avg_risk_score:.1f}%")
    print(f"  Short-term VaR (total) : £{total_short_var:,.2f}")
    print(f"  Long-term VaR (total)  : £{total_long_var:,.2f}")
    print(f"  Projected annual save  : £{annual_recovery:,.2f} (at {assumed_save_rate:.0%} save rate)")
    print(f"  Risk distribution      : {risk_distribution}")
    # print(f"\n  Report payload → {out}")
    print("───────────────────────────────────────────────────────────\n")


    return payload













# The 'Key' is the full path inside the bucket
# df = get_data_from_s3(target_file)

# if df is not None:
#     print("Success! Data loaded:")
# import argparse

if __name__ == "__main__":

    client = "demo_example"
    raw_file = "members_26042026.csv"


    payload = run_pipeline(client = client, 
                           raw_file = raw_file)
    
    # 