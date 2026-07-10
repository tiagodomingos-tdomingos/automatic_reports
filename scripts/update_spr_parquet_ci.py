"""
update_spr_parquet_ci.py
Versão para GitHub Actions — usa variáveis de ambiente para auth.
Env vars obrigatórias:
  GCP_SA_KEY   → conteúdo JSON da service account
  GRID_TOKEN   → grid_sk_... token
"""

import os, json, io, time, sys, requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── CONFIG ────────────────────────────────────────────────────────────────────
BQ_PROJECT   = "meli-bi-data"
GRID_DOC_ID  = "01KWZ4C3XED67DE0RRZ6TNAY2X"
DATASET_NAME = "spr_parquet"
GRID_API     = "https://grid.melioffice.com/api/v1"

SQL = """
WITH OPT AS (
  SELECT
    SIT_SITE_ID,
    SHP_ADJUST_FACILITY_ID,
    SHP_ADJUST_PLANNING_DATE,
    SHP_ADJUST_CYCLE_NAME,
    CAST(JSON_EXTRACT_SCALAR(svc, '$.id') AS INT64) AS SERVICE_ID,
    ROUND(MAX(IF(JSON_EXTRACT_SCALAR(metric, '$.name') = 'spr',
      CAST(JSON_EXTRACT_SCALAR(metric, '$.global') AS FLOAT64), NULL)),0) AS SPR
  FROM `meli-bi-data.WHOWNER.BT_SHP_FP_OPT_TAC_ADJUSTMENT` opt,
    UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT_SCALAR(SHP_ADJUST_SUGGESTION, '$'), '$.simulation.services')) AS SVC,
    UNNEST(JSON_EXTRACT_ARRAY(svc, '$.metrics')) AS METRIC
  WHERE SHP_ADJUST_STATUS = 'finished'
    AND SHP_ADJUST_PLANNING_DATE IS NOT NULL
  GROUP BY 1,2,3,4,5
  ORDER BY SHP_ADJUST_PLANNING_DATE DESC
)
SELECT
  OPT.SIT_SITE_ID,
  SHP_ADJUST_FACILITY_ID,
  SHP_ADJUST_PLANNING_DATE,
  SHP_ADJUST_CYCLE_NAME,
  SERVICE_ID,
  MS.SHP_SRV_MEL_SERVICE_DESC,
  SPR
FROM OPT
LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_SRV_MEL_SERVICE` MS
  ON MS.SHP_SRV_MEL_SERVICE_ID = OPT.SERVICE_ID
WHERE 1=1
"""

# ── BQ AUTH via Service Account ───────────────────────────────────────────────
def get_bq_token():
    from google.oauth2 import service_account
    import google.auth.transport.requests, base64
    raw = os.environ["GCP_SA_KEY"].strip().lstrip('﻿').lstrip('\xef\xbb\xbf')
    # Support both plain JSON and base64-encoded JSON
    try:
        sa_info = json.loads(raw)
    except Exception:
        sa_info = json.loads(base64.b64decode(raw).decode("utf-8-sig"))
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token


# ── BQ QUERY ──────────────────────────────────────────────────────────────────
def bq_query(token, sql, project):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Submit job
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs",
        headers=headers,
        json={"configuration": {"query": {"query": sql, "useLegacySql": False}}},
        timeout=60
    )
    r.raise_for_status()
    job_id = r.json()["jobReference"]["jobId"]
    print(f"  BQ job: {job_id}")
    # Poll
    while True:
        s = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/{job_id}",
            headers=headers, timeout=30
        ).json()["status"]
        if s["state"] == "DONE":
            if "errorResult" in s:
                raise RuntimeError(f"BQ error: {s['errorResult']}")
            break
        print(f"  status: {s['state']}..."); time.sleep(5)
    # Paginate
    schema, rows, page_token, page = [], [], None, 0
    while True:
        params = {"maxResults": 100000}
        if page_token: params["pageToken"] = page_token
        data = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries/{job_id}",
            headers=headers, params=params, timeout=60
        ).json()
        if not schema:
            schema = [f["name"] for f in data["schema"]["fields"]]
        for row in data.get("rows", []):
            rows.append({schema[i]: v.get("v") for i, v in enumerate(row["f"])})
        page_token = data.get("pageToken")
        page += 1
        print(f"  page {page}: {len(rows):,} rows")
        if not page_token: break
    return schema, rows


# ── PARQUET ───────────────────────────────────────────────────────────────────
def to_parquet(schema, rows):
    df = pd.DataFrame(rows, columns=schema)
    df["SERVICE_ID"] = pd.to_numeric(df["SERVICE_ID"], errors="coerce")
    df["SPR"]        = pd.to_numeric(df["SPR"],        errors="coerce")
    df["SHP_ADJUST_PLANNING_DATE"] = pd.to_datetime(
        df["SHP_ADJUST_PLANNING_DATE"], errors="coerce"
    ).dt.date.astype(str)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf, compression="snappy")
    buf.seek(0)
    return buf.read()


# ── GRID UPLOAD ───────────────────────────────────────────────────────────────
def grid_upload(doc_id, dataset_name, parquet_bytes):
    token   = os.environ["GRID_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}

    # Ensure dataset definition exists
    r = requests.post(
        f"{GRID_API}/documents/{doc_id}/datasets/{dataset_name}",
        headers=headers,
        json={"source_type":"external_push","refresh_mode":"external",
              "format":"parquet","source":"github_actions"},
        timeout=30
    )
    if r.status_code not in (200, 201, 409): r.raise_for_status()
    print(f"  Dataset definition: {r.status_code}")

    # Upload URL
    r = requests.post(
        f"{GRID_API}/documents/{doc_id}/datasets/{dataset_name}/upload-url",
        headers=headers, timeout=30
    )
    r.raise_for_status()
    info = r.json()
    revision, upload_url, publish_url = info["revision"], info["upload_url"], info["publish_url"]
    print(f"  Revision: {revision}")

    # PUT Parquet (with retry)
    for attempt in range(3):
        try:
            r = requests.put(upload_url, data=parquet_bytes,
                             headers={"Content-Type":"application/octet-stream"}, timeout=180)
            r.raise_for_status()
            print(f"  Uploaded: {len(parquet_bytes)/1024/1024:.1f} MB")
            break
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt == 2: raise
            time.sleep(5)

    # Publish
    r = requests.post(
        f"https://grid.melioffice.com{publish_url}",
        headers=headers, json={"revision": revision}, timeout=30
    )
    r.raise_for_status()
    print(f"  Published: {r.json().get('publish_state','ok')}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    import datetime
    print(f"\n=== SPR Update — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ===")

    print("1. BQ token...")
    token = get_bq_token()

    print("2. BQ query...")
    schema, rows = bq_query(token, SQL, BQ_PROJECT)
    print(f"   Total: {len(rows):,} rows")

    print("3. Parquet...")
    pb = to_parquet(schema, rows)
    print(f"   Size: {len(pb)/1024/1024:.1f} MB")

    print("4. Grid upload...")
    grid_upload(GRID_DOC_ID, DATASET_NAME, pb)

    print("\n✓ Done!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ {e}", file=sys.stderr)
        sys.exit(1)
