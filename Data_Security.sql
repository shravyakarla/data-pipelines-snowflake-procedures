CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.APPLY_PII_MASKING("DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','tabulate')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session
import re
import pandas as pd

PII_PATTERNS = {
    "EMAIL": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}",
    "PHONE": r"\\b(\\+?\\d{1,3}[\\s-]?)?(\\(?\\d{3,5}\\)?[\\s-]?)?\\d{3,5}[\\s-]?\\d{3,5}\\b",
    "AADHAAR": r"\\b\\d{4}\\s\\d{4}\\s\\d{4}\\b",
    "CREDIT_CARD": r"\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b"
}
 
def mask_value(value, pii_type):
    if not isinstance(value, str):
        return value
    if pii_type == "EMAIL":
        parts = value.split("@")
        if len(parts) == 2:
            return parts[0][:1] + "***@" + parts[1]
    elif pii_type == "PHONE":
        return re.sub(r"\\d(?=\\d{4})", "*", value)
    elif pii_type == "AADHAAR":
        return "XXXX XXXX " + value[-4:]
    elif pii_type == "CREDIT_CARD":
        return "XXXX-XXXX-XXXX-" + value[-4:]
    return "*****"
 
def detect_pii(value):
    if not value:
        return None
    for pii_type, pattern in PII_PATTERNS.items():
        if re.search(pattern, str(value)):
            return pii_type
    return None
 
def main(session: Session, DB_NAME: "MY_DB", SCHEMA_NAME: "PUBLIC", TABLE_NAME: "FACT_CUSTOMER") -> str:
    full_table = f"{DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"
    masked_table = f"{DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}_MASKED"
 
    df = session.table(full_table)
    columns = df.columns
    pdf = df.limit(1000).to_pandas()
 
    pii_info = {}
    for col in columns:
        detected_types = set()
        for val in pdf[col].dropna().astype(str).head(100):
            pii_type = detect_pii(val)
            if pii_type:
                detected_types.add(pii_type)
        if detected_types:
            pii_info[col] = ", ".join(detected_types)
 
    if not pii_info:
        return f"No PII detected in table {TABLE_NAME}"
 
    for col, pii_type in pii_info.items():
        pdf[col] = pdf[col].apply(lambda x: mask_value(str(x), pii_type.split(",")[0]) if pd.notna(x) else x)
 
    pdf["PII_MASKING_TYPE"] = str(pii_info)
    session.create_dataframe(pdf).write.save_as_table(masked_table, mode="overwrite")
 
    # ---- Get before and after samples ----
    before_df = session.sql(f"SELECT * FROM {full_table} LIMIT 5").to_pandas()
    after_df = session.sql(f"SELECT * FROM {masked_table} LIMIT 5").to_pandas()
 
    before_html = before_df.to_markdown(index=False)
    after_html = after_df.to_markdown(index=False)
 
    result = (
        f"### 🧾 Original Table Sample ({TABLE_NAME})\\n"
        f"{before_html}\\n\\n"
        f"### 🔒 Masked Table Sample ({TABLE_NAME}_MASKED)\\n"
        f"{after_html}\\n\\n"
        f"**Columns Masked:** {list(pii_info.keys())}\\n"
        f"**Masking Details:** {pii_info}"
    )
 
    return result
';