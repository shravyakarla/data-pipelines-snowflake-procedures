CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.CODE_GENERATOR_SP("OBJECTIVE" VARCHAR, "METADATA_JSON" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','streamlit')
HANDLER = 'run'
IMPORTS = ('@MY_DB.PUBLIC.AGENT_STAGE/core.zip')
EXECUTE AS OWNER
AS '
import json
from datetime import datetime
from core.agent import CodeGenerator
from core.helpers import clean_sql

def run(session: ''snowflake.snowpark.Session'', OBJECTIVE: str, METADATA_JSON: str):
    start_ts = datetime.now()
    METADATA = json.loads(METADATA_JSON)

    # Normalize prompt
    objective_lower = OBJECTIVE.strip().lower()

    # -----------------------------
    # 1️⃣ Determine prompt type
    # -----------------------------
    if "scd1" in objective_lower or "incremental" in objective_lower:
        task_type = "scd1_pipeline"
    elif "join" in objective_lower:
        task_type = "join_query"
    elif "summary" in objective_lower or "aggregate" in objective_lower or "monthly" in objective_lower:
        task_type = "aggregation"
    elif "filename:" in objective_lower and "stage:" in objective_lower:
        task_type = "ddl_copy"
    else:
        task_type = "generic"

    # -----------------------------
    # 2️⃣ Build specialized prompts
    # -----------------------------
    if task_type == "scd1_pipeline":
        sql_prompt = f"""
        Generate a Snowflake SQL MERGE statement implementing an incremental load with SCD Type 1 (overwrite) logic.
        Use metadata for column names and keys.

        Requirements:
        - Handle cases where multiple source rows exist for the same key; use only the latest record based on CREATED_AT.
        - Use Snowflake''s modern MERGE syntax with `UPDATE ALL BY NAME` and `INSERT ALL BY NAME` if possible.
        - Ensure the logic updates existing records with latest data (SCD Type 1) and inserts new ones.
        - Output only valid SQL code without explanations.

        Objective: {OBJECTIVE}
        """

        snowpark_prompt = f"""
        Generate Python Snowpark DataFrame code implementing incremental load logic with SCD Type 1.
        Ensure that for duplicate keys, only the latest record (based on CREATED_AT) is retained before merging.
        Implement logic equivalent to Snowflake MERGE using `UPDATE ALL BY NAME` and `INSERT ALL BY NAME` if supported.
        Output only Snowpark code.
        Objective: {OBJECTIVE}
        """

    elif task_type == "join_query":
        sql_prompt = f"""
        Generate a Snowflake SQL SELECT query joining the necessary tables as described in the objective.
        Include all join keys, derived columns, and computed fields.
        Output only SQL code.
        Objective: {OBJECTIVE}
        """

        snowpark_prompt = f"""
        Generate Python Snowpark DataFrame code that performs the same join and column selection.
        Output only Snowpark code.
        Objective: {OBJECTIVE}
        """

    elif task_type == "aggregation":
        sql_prompt = f"""
        Generate a Snowflake SQL SELECT statement that performs the requested aggregation or summary.
        Include GROUP BY and computed metrics.
        Output only SQL code.
        Objective: {OBJECTIVE}
        """

        snowpark_prompt = f"""
        Generate Python Snowpark DataFrame code implementing the same aggregation or summary logic.
        Output only Snowpark code.
        Objective: {OBJECTIVE}
        """

    elif task_type == "ddl_copy":
        sql_prompt = f"""
        The input specifies a file and stage name in the format: filename:<file> stage:<stage>.
        Generate:
          1. A CREATE OR REPLACE TABLE DDL statement inferred from the file (use reasonable column types if metadata missing).
          2. A COPY INTO command to load data from the given stage and file into that table.
        Output only valid SQL code (both commands).
        Objective: {OBJECTIVE}
        """

        snowpark_prompt = f"""
        Generate Python Snowpark DataFrame code that:
          1. Creates the target table if not exists (DDL),
          2. Reads the staged file using session.read.option(''pattern'', ...) or copy-like logic,
          3. Writes the data into the target table.
        Output only Snowpark code.
        Objective: {OBJECTIVE}
        """

    else:
        sql_prompt = f"""
        Generate the most appropriate Snowflake SQL code for the following objective.
        Output only SQL.
        Objective: {OBJECTIVE}
        """

        snowpark_prompt = f"""
        Generate Python Snowpark DataFrame code implementing the same logic.
        Output only Snowpark code.
        Objective: {OBJECTIVE}
        """

    # -----------------------------
    # 3️⃣ Call CodeGenerator twice
    # -----------------------------
    sql_result = CodeGenerator.generate_reply(
        [{"content": sql_prompt}],
        session=session,
        metadata=METADATA
    )
    sql_code = clean_sql(sql_result.get("sql_code") if isinstance(sql_result, dict) else sql_result)

    snowpark_result = CodeGenerator.generate_reply(
        [{"content": snowpark_prompt}],
        session=session,
        metadata=METADATA
    )
    snowpark_code = clean_sql(snowpark_result.get("snowpark_code") if isinstance(snowpark_result, dict) else snowpark_result)

    # -----------------------------
    # 4️⃣ Logging
    # -----------------------------
    end_ts = datetime.now()
    duration_seconds = round((end_ts - start_ts).total_seconds(), 2)

    agent_log = {
        "agent_name": "CodeGenerator",
        "task_type": task_type,
        "start_ts": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "end_ts": end_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration_seconds,
        "input_prompt": OBJECTIVE,
        "sql_preview": str(sql_code)[:300],
        "snowpark_preview": str(snowpark_code)[:300]
    }

    # -----------------------------
    # 5️⃣ Return both codes
    # -----------------------------
    return {
        "task_type": task_type,
        "sql_code": sql_code,
        "snowpark_code": snowpark_code,
        "agent_log": agent_log
    }
';