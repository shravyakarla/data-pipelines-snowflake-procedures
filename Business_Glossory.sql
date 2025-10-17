CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.GENERATE_BUSINESS_GLOSSARY_PY("DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session
 
def escape_quotes(text: str) -> str:
    return text.replace("''", "''''") if text else text
 
 
def main(session: Session, DB_NAME: "MY_DB", SCHEMA_NAME: "PUBLIC", TABLE_NAME: "FACT_CUSTOMER") -> str:
    glossary_table = f"{DB_NAME}.{SCHEMA_NAME}.BUSINESS_GLOSSARY"
 
    # ✅ Create glossary table without COLUMN_COMMENT
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {glossary_table} (
            TABLE_NAME STRING,
            COLUMN_NAME STRING,
            DATA_TYPE STRING,
            BUSINESS_DEFINITION STRING
        )
    """).collect()
 
    # ✅ Fetch previous glossary entries for return message
    prev_query = f"SELECT * FROM {glossary_table}"
    if TABLE_NAME:
        prev_query += f" WHERE TABLE_NAME = ''{TABLE_NAME.upper()}''"
    prev_entries = session.sql(prev_query).collect()
    prev_summary = "\\n".join([f"{r[''TABLE_NAME'']}.{r[''COLUMN_NAME'']}: {r[''BUSINESS_DEFINITION'']}" for r in prev_entries]) or "No previous entries found."
 
    # ✅ Build metadata query
    filter_condition = f"AND TABLE_NAME = ''{TABLE_NAME.upper()}''" if TABLE_NAME else ""
    query = f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM {DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''{SCHEMA_NAME.upper()}''
        {filter_condition}
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    df = session.sql(query).collect()
 
    # ✅ Clear previous glossary entries
    if TABLE_NAME:
        session.sql(f"DELETE FROM {glossary_table} WHERE TABLE_NAME = ''{TABLE_NAME.upper()}''").collect()
    else:
        session.sql(f"DELETE FROM {glossary_table}").collect()
 
    # ✅ Generate and insert new definitions
    for row in df:
        table_name = row["TABLE_NAME"]
        column_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"]
 
        table_name_esc = escape_quotes(table_name)
        column_name_esc = escape_quotes(column_name)
        data_type_esc = escape_quotes(data_type)
 
        prompt = (
            f"Generate a short, clear, business-friendly definition for the column ''{column_name_esc}'' "
            f"in the table ''{table_name_esc}''. The data type is ''{data_type_esc}''."
        )
        prompt_esc = escape_quotes(prompt)
 
        # 🔹 Cortex call
        cortex_sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                ''openai-gpt-4.1'',
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(''role'', ''user'', ''content'', ''{prompt_esc}'')
                ),
                OBJECT_CONSTRUCT(''temperature'', 0.3)
            ) AS RESPONSE
        """
        try:
            result = session.sql(cortex_sql).collect()
            business_def = result[0]["RESPONSE"] if result else "Definition not generated"
        except Exception as e:
            business_def = f"Error generating definition: {str(e)}"
 
        # ✅ Insert into BUSINESS_GLOSSARY
        insert_sql = f"""
            INSERT INTO {glossary_table} (TABLE_NAME, COLUMN_NAME, DATA_TYPE, BUSINESS_DEFINITION)
            VALUES (''{table_name_esc}'', ''{column_name_esc}'', ''{data_type_esc}'', ''{escape_quotes(business_def)}'')
        """
        session.sql(insert_sql).collect()
 
    # ✅ Return success + previous glossary summary
    return f"""✅ AI-powered glossary generated successfully for schema ''{SCHEMA_NAME}'' {''and table '' + TABLE_NAME if TABLE_NAME else ''(all tables)''}.
    
🕘 Previous Glossary Entries:
{prev_summary}
"""
';