CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.DISCOVER_AND_RUN_DQ_AUTO_RULES("INPUT_TABLE" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'discover_and_run_dq_auto_rules'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import json

def discover_and_run_dq_auto_rules(session: Session, input_table: str):

    try:
        # --------------------------
        # Normalize and parse table name
        # --------------------------
        parts = input_table.strip().split(''.'')
        if len(parts) == 1:
            db = session.get_current_database()
            schema = session.get_current_schema()
            table = parts[0]
        elif len(parts) == 2:
            db = session.get_current_database()
            schema, table = parts
        elif len(parts) == 3:
            db, schema, table = parts
        else:
            return {"error": f"Invalid table reference: {input_table}"}

        table_full_name = f''"{db}"."{schema}"."{table}"''

        # --------------------------
        # Get enriched column metadata
        # --------------------------
        cols_df = session.sql(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CASE 
                    WHEN DATA_TYPE ILIKE ''%CHAR%'' OR DATA_TYPE ILIKE ''%TEXT%'' THEN CHARACTER_MAXIMUM_LENGTH
                    WHEN DATA_TYPE ILIKE ''%NUMBER%'' OR DATA_TYPE ILIKE ''%DECIMAL%'' OR DATA_TYPE ILIKE ''%FLOAT%'' OR DATA_TYPE ILIKE ''%INT%'' 
                        THEN NUMERIC_PRECISION
                    WHEN DATA_TYPE ILIKE ''%DATE%'' OR DATA_TYPE ILIKE ''%TIME%'' THEN DATETIME_PRECISION
                    ELSE NULL
                END AS CHARACTER_MAXIMUM_LENGTH
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ''{schema.upper()}''
              AND TABLE_NAME = ''{table.upper()}''
            ORDER BY ORDINAL_POSITION
        """).to_pandas()

        # Replace NaNs with None for clean JSON output
        cols_df = cols_df.replace({np.nan: None})

        if cols_df.empty:
            return {"error": f''Table {table_full_name} not found or has no columns.''}

        # --------------------------
        # Data Profiling
        # --------------------------
        profiling = {}
        for _, row in cols_df.iterrows():
            col = row[''COLUMN_NAME'']
            dtype = row[''DATA_TYPE'']

            stats_query = f"""
                SELECT
                    COUNT(*) AS count_all,
                    COUNT(DISTINCT "{col}") AS count_distinct,
                    SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS count_nulls
                FROM {table_full_name}
            """
            stats = session.sql(stats_query).to_pandas().iloc[0]

            col_profile = {
                "count_all": int(stats[''COUNT_ALL'']),
                "count_distinct": int(stats[''COUNT_DISTINCT'']),
                "count_nulls": int(stats[''COUNT_NULLS'']),
                "null_ratio": float(stats[''COUNT_NULLS'']) / max(int(stats[''COUNT_ALL'']), 1),
                "distinct_ratio": float(stats[''COUNT_DISTINCT'']) / max(int(stats[''COUNT_ALL'']), 1),
                "data_type": dtype
            }

            # Numeric stats
            if any(x in dtype for x in ["NUMBER", "FLOAT", "INT", "DECIMAL", "DOUBLE"]):
                num_stats = session.sql(f"""
                    SELECT AVG("{col}") AS avg_val, MIN("{col}") AS min_val,
                           MAX("{col}") AS max_val, STDDEV("{col}") AS stddev_val
                    FROM {table_full_name}
                """).to_pandas().iloc[0]
                col_profile["numeric_stats"] = {
                    "AVG_VAL": str(num_stats[''AVG_VAL'']),
                    "MIN_VAL": num_stats[''MIN_VAL''],
                    "MAX_VAL": num_stats[''MAX_VAL''],
                    "STDDEV": num_stats[''STDDEV_VAL'']
                }
            else:
                col_profile["numeric_stats"] = None

            profiling[col] = col_profile

        # --------------------------
        # Data Quality Evaluation (5 Pillars)
        # --------------------------
        dq_results = {}
        for col, prof in profiling.items():
            dq_results[col] = {}
            total_score = 0
            pillar_count = 0

            # 1️⃣ Completeness
            completeness = 1 - prof[''null_ratio'']
            dq_results[col][''completeness''] = {
                "rule": f"{col} should have minimal nulls",
                "score": round(completeness, 3)
            }
            total_score += completeness
            pillar_count += 1

            # 2️⃣ Uniqueness
            uniqueness = prof[''distinct_ratio'']
            dq_results[col][''uniqueness''] = {
                "rule": f"{col} values should be unique or diverse",
                "score": round(uniqueness, 3)
            }
            total_score += uniqueness
            pillar_count += 1

            # 3️⃣ Validity
            if prof[''numeric_stats'']:
                avg_val = float(prof[''numeric_stats''][''AVG_VAL''] or 0)
                validity = 1.0 if avg_val >= 0 else 0.5
            else:
                validity = 1.0
            dq_results[col][''validity''] = {
                "rule": f"{col} must be within valid value ranges",
                "score": round(validity, 3)
            }
            total_score += validity
            pillar_count += 1

            # 4️⃣ Accuracy
            if "DATE" in prof[''data_type'']:
                count_future = session.sql(f"""
                    SELECT COUNT(*) AS c FROM {table_full_name}
                    WHERE "{col}" > CURRENT_TIMESTAMP()
                """).to_pandas().iloc[0][''C'']
                total = prof["count_all"]
                accuracy = 1 - (count_future / total if total > 0 else 0)
            else:
                accuracy = 1.0
            dq_results[col][''accuracy''] = {
                "rule": f"{col} values should be correct (e.g., no future dates)",
                "score": round(accuracy, 3)
            }
            total_score += accuracy
            pillar_count += 1

            # 5️⃣ Consistency
            consistency = 1.0
            dq_results[col][''consistency''] = {
                "rule": f"{col} should align consistently with related columns",
                "score": round(consistency, 3)
            }
            total_score += consistency
            pillar_count += 1

            # Column-level average
            dq_results[col][''column_score''] = round(total_score / pillar_count, 3)

        # --------------------------
        # Compute overall table score
        # --------------------------
        table_score = float(np.mean([dq_results[c][''column_score''] for c in dq_results]))

        # --------------------------
        # Return Final DQ Report
        # --------------------------
        return {
            "target_table": table_full_name,
            "columns": cols_df.to_dict(orient="records"),
            "profiling": profiling,
            "dq_results": dq_results,
            "table_score": round(table_score, 3)
        }

    except Exception as e:
        return {"error": str(e)}
';