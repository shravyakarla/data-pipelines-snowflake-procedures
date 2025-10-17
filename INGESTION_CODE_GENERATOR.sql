CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.INGESTION_CODE_GENERATORS("OBJECTIVE" VARCHAR, "FILE_PATH" VARCHAR)

RETURNS VARIANT

LANGUAGE PYTHON

RUNTIME_VERSION = '3.11'

PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl','pyarrow')

HANDLER = 'run'

EXECUTE AS OWNER

AS '
 
import json
 
from datetime import datetime

def run(session, OBJECTIVE: str, FILE_PATH: str):

    start_ts = datetime.now()
 
    agent_name = "IngestionPipelineGenerator"

    try:
 
        # 1️⃣ Run DQ Discovery procedure to extract metadata
 
        dq_result_raw = session.call("PUBLIC.DQ_DISCOVERY_AND_CHECK", FILE_PATH)

        # Parse string to dict if needed
 
        dq_result = dq_result_raw if isinstance(dq_result_raw, dict) else json.loads(dq_result_raw)

        if "error" in dq_result:
 
            raise Exception(f"DQ Discovery failed: {dq_result[''error'']}")

        metadata_json = json.dumps(dq_result.get("file_definition", {}))

        # 2️⃣ Call the Code Generator with metadata and objective
 
        codegen_result_raw = session.call("PUBLIC.CODE_GENERATOR", OBJECTIVE, metadata_json)

        # Parse again if necessary
 
        codegen_result = codegen_result_raw if isinstance(codegen_result_raw, dict) else json.loads(codegen_result_raw)

        sql_code = codegen_result.get("sql_code")
 
        snowpark_code = codegen_result.get("snowpark_code")

        end_ts = datetime.now()
 
        duration_seconds = round((end_ts - start_ts).total_seconds(), 2)

        # 3️⃣ Build a combined response
 
        result = {
 
            "status": "SUCCESS",
 
            "file_path": FILE_PATH,
 
            "objective": OBJECTIVE,
 
            "sql_code": sql_code,
 
            "snowpark_code": snowpark_code,
 
            "dq_summary": dq_result.get("audit_log", {}),
 
            "execution_time_sec": duration_seconds
 
        }

        return result

    except Exception as e:
 
        return {
 
            "status": "FAILED",
 
            "error": str(e),
 
            "agent_name": agent_name
 
        }
 
';
 