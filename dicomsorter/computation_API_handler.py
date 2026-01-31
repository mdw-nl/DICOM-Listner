from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dicomsorter import PostgresInterface
from config_handler import Config

class ModalityRequest(BaseModel):
    modality: str

app = FastAPI()

# Database connection
config_dict_db = Config("postgres").config
host, port, user, pwd, db_name = (
    config_dict_db["host"],
    config_dict_db["port"],
    config_dict_db["username"],
    config_dict_db["password"],
    config_dict_db["db"],
)
db = PostgresInterface(host=host, database=db_name, user=user, password=pwd, port=port)
db.connect()

# Ensure calculation_status table exists
db.execute_query("""
CREATE TABLE IF NOT EXISTS calculation_status (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sop_instance_uid TEXT,
    status BOOLEAN NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
""")

@app.post("/sop_instance_uids")
def get_new_sop_instance_uids(request: ModalityRequest):
    modality = request.modality

    try:
        # Select SOPs not yet marked as sent for this modality
        sql_query = """
        SELECT sop_instance_uid
        FROM dicom_insert
        WHERE modality = %s
        AND sop_instance_uid NOT IN (
            SELECT sop_instance_uid
            FROM calculation_status
            WHERE status = TRUE AND modality = %s
        )
        """
        results = db.fetch_all(sql_query, (modality, modality))
        new_sops = [row[0] for row in results] if results else []

        # Mark returned SOPs as sent in calculation_status
        for sop in new_sops:
            db.execute_query(
                """
                INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp)
                VALUES (%s, %s, TRUE, %s)
                """,
                (sop, modality, datetime.utcnow())
            )

        return {"modality": modality, "new_sop_instance_uids": new_sops}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
