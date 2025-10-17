CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.EXECUTION_ENGINE("SQL_CODE" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
import json
import re
from datetime import datetime

def run(session, SQL_CODE: str):
    agent_name = "ExecutionEngine"
    start_time = datetime.now()

    try:
        if not SQL_CODE or not SQL_CODE.strip():
            return {
                "status": "FAILED",
                "agent_name": agent_name,
                "error": "SQL_CODE input is empty or null"
            }

        # Remove comments and blank lines
        cleaned_lines = [
            line for line in SQL_CODE.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        cleaned_sql = "\\n".join(cleaned_lines)

        # Split SQL safely by semicolons, ignoring semicolons inside strings
        def split_sql_statements(sql_text):
            stmts, current, in_single, in_double = [], [], False, False
            for ch in sql_text:
                if ch == "''" and not in_double:
                    in_single = not in_single
                elif ch == "\\"":
                    continue  # skip escaped quote
                elif ch == "\\"" and not in_single:
                    in_double = not in_double
                elif ch == ";" and not in_single and not in_double:
                    stmt = "".join(current).strip()
                    if stmt:
                        stmts.append(stmt)
                    current = []
                    continue
                current.append(ch)
            last_stmt = "".join(current).strip()
            if last_stmt:
                stmts.append(last_stmt)
            return stmts

        sql_statements = split_sql_statements(cleaned_sql)

        results = []
        success_count = 0
        fail_count = 0
        preview_tables = {}
        preview_limit = 5
        detected_tables = set()

        for i, stmt in enumerate(sql_statements, start=1):
            stmt_start = datetime.now()
            try:
                session.sql(stmt).collect()
                results.append({
                    "step": i,
                    "status": "SUCCESS",
                    "sql": stmt,
                    "error": None,
                    "execution_time_sec": round((datetime.now() - stmt_start).total_seconds(), 2)
                })
                success_count += 1

                # Detect table names from common SQL patterns
                table_matches = re.findall(r"(?:from|join|into|update|table)\\s+([a-zA-Z0-9_\\.]+)", stmt, re.IGNORECASE)
                for table_name in table_matches:
                    if len(preview_tables) < preview_limit and table_name not in detected_tables:
                        detected_tables.add(table_name)
                        try:
                            preview_data = session.sql(f"SELECT * FROM {table_name} LIMIT 5").collect()
                            preview_tables[table_name] = [row.as_dict() for row in preview_data]
                        except Exception as preview_err:
                            preview_tables[table_name] = f"Error fetching preview: {str(preview_err)}"

            except Exception as e:
                results.append({
                    "step": i,
                    "status": "FAILED",
                    "sql": stmt,
                    "error": str(e),
                    "execution_time_sec": round((datetime.now() - stmt_start).total_seconds(), 2)
                })
                fail_count += 1

        duration = round((datetime.now() - start_time).total_seconds(), 2)

        return {
            "status": "COMPLETED",
            "agent_name": agent_name,
            "total_statements": len(sql_statements),
            "success_count": success_count,
            "failed_count": fail_count,
            "execution_time_sec": duration,
            "details": results,
            "table_previews": preview_tables
        }

    except Exception as e:
        return {
            "status": "FAILED",
            "agent_name": agent_name,
            "error": str(e)
        }
';