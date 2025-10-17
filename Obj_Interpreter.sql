CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.OBJ_INTERPRETER("OBJECTIVE" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','streamlit')
HANDLER = 'run'
IMPORTS = ('@MY_DB.PUBLIC.AGENT_STAGE/core.zip')
EXECUTE AS OWNER
AS '
import json
import time
from datetime import datetime
from core.cortex_client import fetch_snowflake_metadata
from core.helpers import format_metadata_for_prompt
from core.agent import ObjectiveInterpreter

def filter_required_metadata(objective: str, metadata: dict):
    required_tables = []
    numeric_keywords = ["SUM", "AVG", "COUNT", "AMOUNT", "PRICE", "QUANTITY", "TOTAL"]
    objective_upper = objective.upper()

    for table, cols in metadata.get("columns", {}).items():
        for col_name, dtype in cols:
            if col_name.upper() in objective_upper or any(k in col_name.upper() for k in numeric_keywords):
                required_tables.append(table)
                break

    filtered_columns = {t: metadata["columns"][t] for t in required_tables}
    return {"tables": required_tables, "columns": filtered_columns}

def run(session: ''snowflake.snowpark.Session'', OBJECTIVE: str):
    start_ts = datetime.now()
    
    # Step 1: Fetch full metadata
    full_metadata = fetch_snowflake_metadata(session)

    # Step 2: Define prompt for agent
    prompt = f"""
You are the Objective Interpreter.
Rules:
- Only include tables necessary for numeric aggregates mentioned in the objective.
- Do NOT add descriptive tables unless explicitly required.
- Return SQL template and explanation using only these tables.

User Objective:
{OBJECTIVE}
"""

    # Step 3: Call ObjectiveInterpreter
    interpreted_goal = ObjectiveInterpreter.generate_reply(
        [{"content": prompt}],
        session=session,
        metadata=full_metadata
    )

    # Step 4: Filter metadata
    filtered_metadata = filter_required_metadata(interpreted_goal, full_metadata)

    end_ts = datetime.now()

    # Step 5: Build agent log
    agent_log = {
        "agent_name": "ObjectiveInterpreter",
        "start_ts": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "end_ts": end_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": round((end_ts - start_ts).total_seconds(), 2),
        "input_prompt": OBJECTIVE,
        "result_summary": str(interpreted_goal)[:500]
    }

    return {
        "objective": OBJECTIVE,
        "interpreted_goal": interpreted_goal,
        "metadata": filtered_metadata,
        "agent_log": agent_log
    }
';