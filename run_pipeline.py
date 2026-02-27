import subprocess
import sys
import pymysql
from datetime import datetime
import streamlit as st

def update_status(status, message=None):
    cur.execute(
        """
        UPDATE pipeline_runs
        SET status=%s,
            message=%s,
            started_at=IF(%s='RUNNING', NOW(), started_at),
            finished_at=IF(%s IN ('SUCCESS','FAILED'), NOW(), finished_at)
        WHERE id=1
        """,
        (status, message, status, status)
    )
    conn.commit()

conn = pymysql.connect(
    host="localhost",
    user="llm_reader",
    password=st.secrets["mysql"]["password"],
    database="world"
)
cur = conn.cursor()

try:
    update_status("RUNNING", "Pipeline started")

    subprocess.check_call(["python", "weekly_runner_mysql.py"])
    subprocess.check_call(["python", "tagger_mysql.py"])

    update_status("SUCCESS", "Pipeline completed successfully")

except Exception as e:
    update_status("FAILED", str(e))
    sys.exit(1)

finally:
    conn.close()