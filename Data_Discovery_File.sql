CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.DQ_DISCOVERY_AND_CHECKS("FILE_PATH" VARCHAR)

RETURNS VARIANT

LANGUAGE PYTHON

RUNTIME_VERSION = '3.9'

PACKAGES = ('pandas','openpyxl','pyarrow','lxml','snowflake-snowpark-python')

HANDLER = 'main'

EXECUTE AS CALLER

AS '

import pandas as pd

import time

import os

import tempfile
 
# ---------- Utility: Map Pandas dtype to Snowflake dtype ----------

def map_dtype_to_snowflake(dtype):

    dtype = str(dtype)

    if dtype in [''object'', ''string'']:

        return ''VARCHAR''

    elif dtype in [''int64'', ''int32'']:

        return ''NUMBER''

    elif dtype in [''float64'', ''float32'']:

        return ''FLOAT''

    elif dtype == ''bool'':

        return ''BOOLEAN''

    elif ''datetime'' in dtype:

        return ''TIMESTAMP''

    else:

        return ''VARIANT''
 
# ---------- Utility: File size formatter ----------

def get_file_size(size_bytes):

    if size_bytes < 1024:

        return f"{size_bytes} B"

    elif size_bytes < 1024**2:

        return f"{round(size_bytes/1024, 2)} KB"

    else:

        return f"{round(size_bytes/(1024**2), 2)} MB"
 
# ---------- File metadata extraction ----------

def get_file_metadata(df, file_name, file_type, file_size):

    columns = [

        {"column_name": col, "type": map_dtype_to_snowflake(dtype)}

        for col, dtype in df.dtypes.items()

    ]

    return {

        "file_name": file_name,

        "file_type": file_type,

        "file_size": file_size,

        "columns": columns

    }
 
# ---------- Consistency Checks ----------

def consistency_checks(df):

    results = []

    for col1 in df.columns:

        for col2 in df.columns:

            if col1 != col2:

                mapping = df[[col1, col2]].dropna().drop_duplicates()

                grouped = mapping.groupby(col1)[col2].nunique()

                inconsistent_keys = grouped[grouped > 1]

                result = 1.0 if len(inconsistent_keys) == 0 else 0.0

                status = "passed" if result == 1.0 else "failed"

                reason = (

                    f"Each value in {col1} maps to one value in {col2}"

                    if status == "passed"

                    else f"{len(inconsistent_keys)} values in {col1} map to multiple values in {col2}"

                )

                results.append({

                    "pillar": "consistency",

                    "column": f"{col1}->{col2}",

                    "result": float(result),

                    "rule": f"Each {col1} maps to a consistent {col2}",

                    "status": status,

                    "reason": reason

                })

    return results
 
# ---------- Accuracy Checks ----------

def accuracy_checks(df):

    results = []

    for col in df.columns:

        if pd.api.types.is_numeric_dtype(df[col]):

            series = df[col].dropna()

            mean = series.mean()

            std = series.std()

            outliers = series[(series - mean).abs() > 3 * std]

            result = 1.0 - len(outliers) / len(series) if len(series) > 0 else 1.0

            status = "passed" if result >= 0.95 else "failed"

            reason = f"{len(outliers)} outliers detected in {col}" if status == "failed" else "Outliers within acceptable range"

            results.append({

                "pillar": "accuracy",

                "column": col,

                "result": float(result),

                "rule": f"Outlier check on {col} (z-score > 3)",

                "status": status,

                "reason": reason

            })

        elif pd.api.types.is_string_dtype(df[col]):

            value_counts = df[col].value_counts(normalize=True)

            rare_values = value_counts[value_counts < 0.01]

            result = 1.0 - rare_values.sum()

            status = "passed" if result >= 0.95 else "failed"

            reason = f"{len(rare_values)} rare values (<1%) in {col}" if status == "failed" else "Rare values within acceptable range"

            results.append({

                "pillar": "accuracy",

                "column": col,

                "result": float(result),

                "rule": f"Rare value check on {col} (<1% frequency)",

                "status": status,

                "reason": reason

            })

    return results
 
# ---------- Completeness, Uniqueness, Validity ----------

def dq_checks(df):

    results = []

    total = len(df)
 
    for col in df.columns:

        # Completeness

        nulls = df[col].isnull().sum()

        completeness = (total - nulls) / total

        status = "passed" if completeness >= 0.95 else "failed"

        reason = f"{nulls} nulls out of {total} rows" if status == "failed" else "Less than 5% nulls"

        results.append({

            "pillar": "completeness",

            "column": col,

            "result": float(completeness),

            "rule": f"Completeness of {col}",

            "status": status,

            "reason": reason

        })
 
        # Uniqueness

        uniqueness = df[col].nunique() / total

        status = "passed" if uniqueness >= 0.95 else "failed"

        reason = f"{df[col].nunique()} unique values out of {total}" if status == "failed" else "High uniqueness"

        results.append({

            "pillar": "uniqueness",

            "column": col,

            "result": float(uniqueness),

            "rule": f"Uniqueness of {col}",

            "status": status,

            "reason": reason

        })
 
        # Validity

        dtype = str(df[col].dtype)

        if dtype == ''object'':

            valid = df[col].apply(lambda x: isinstance(x, str)).all()

        else:

            valid = pd.api.types.is_numeric_dtype(df[col])

        status = "passed" if valid else "failed"

        reason = "All values match expected type" if valid else "Some values do not match expected type"

        results.append({

            "pillar": "validity",

            "column": col,

            "result": str(valid),

            "rule": f"Validity of {col}",

            "status": status,

            "reason": reason

        })
 
    # Combine with other checks

    results.extend(consistency_checks(df))

    results.extend(accuracy_checks(df))

    return results
 
# ---------- File Loader ----------

def load_file(local_file_path):

    ext = os.path.splitext(local_file_path)[-1].lower()

    if ext == ''.csv'':

        return pd.read_csv(local_file_path), ''csv''

    elif ext in [''.xls'', ''.xlsx'']:

        return pd.read_excel(local_file_path), ''excel''

    elif ext == ''.json'':

        try:

            return pd.read_json(local_file_path, lines=True), ''json''

        except:

            return pd.read_json(local_file_path), ''json''

    elif ext == ''.xml'':

        return pd.read_xml(local_file_path), ''xml''

    elif ext == ''.parquet'':

        return pd.read_parquet(local_file_path), ''parquet''

    else:

        raise ValueError(f"Unsupported file format: {ext}")
 
# ---------- Main Procedure ----------

def main(session, file_path):

    start = time.time()

    agent_name = ''DQAgentPython''

    try:

        temp_dir = tempfile.mkdtemp()

        session.file.get(file_path, temp_dir)
 
        file_name = file_path.split(''/'')[-1]

        local_file_path = os.path.join(temp_dir, file_name)
 
        # Compute file size

        file_size = get_file_size(os.path.getsize(local_file_path))
 
        df, file_type = load_file(local_file_path)
 
        metadata = get_file_metadata(df, file_name, file_type, file_size)

        dq_result = dq_checks(df)
 
        passed_rules = [

            {"pillar": r["pillar"], "column": r["column"], "rule": r["rule"], "reason": r["reason"]}

            for r in dq_result if r["status"] == "passed"

        ]
 
        failed_rules = [

            {"pillar": r["pillar"], "column": r["column"], "rule": r["rule"], "reason": r["reason"]}

            for r in dq_result if r["status"] == "failed"

        ]
 
        result_summary = {

            "rules_pass": len(passed_rules),

            "rules_fail": len(failed_rules),

            "passed_rules_detail": passed_rules,

            "failed_rules_detail": failed_rules

        }
 
        end = time.time()

        audit_log = {

            "agent_name": agent_name,

            "start_time": start,

            "end_time": end,

            "input": file_path,

            "result_summary": result_summary

        }
 
        result = {

            "file_definition": metadata,

            "dq_auto_check_result": dq_result,

            "audit_log": audit_log

        }

        return result

    except Exception as e:

        return {"error": str(e)}

';
 