from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config_handler import Config
from dicomsorter.database import PostgresInterface

_NOT_SENT = "AND {alias}.sop_instance_uid NOT IN (SELECT sop_instance_uid FROM calculation_status WHERE modality = '{mod}' AND status = TRUE)"

_RT_CHAIN_CONFIG: dict[str, tuple[str, list[str]]] = {
    "RTDOSE": (
        """
        SELECT dose.sop_instance_uid, dose.patient_id, dose.study_instance_uid,
               plan.sop_instance_uid, struct.sop_instance_uid, struct.referenced_ct_series_uid
        FROM dicom_insert dose
        JOIN dicom_insert plan  ON plan.sop_instance_uid  = dose.referenced_rt_plan_uid      AND plan.modality   = 'RTPLAN'
        JOIN dicom_insert struct ON struct.sop_instance_uid = plan.referenced_rtstruct_sop_uid AND struct.modality = 'RTSTRUCT'
        WHERE dose.modality = 'RTDOSE'
        """
        + _NOT_SENT.format(alias="dose", mod="RTDOSE"),
        ["rtdose_sop_uid", "patient_id", "study_uid", "rtplan_sop_uid", "rtstruct_sop_uid", "ct_series_uid"],
    ),
    "RTPLAN": (
        """
        SELECT plan.sop_instance_uid, plan.patient_id, plan.study_instance_uid,
               struct.sop_instance_uid, struct.referenced_ct_series_uid
        FROM dicom_insert plan
        JOIN dicom_insert struct ON struct.sop_instance_uid = plan.referenced_rtstruct_sop_uid AND struct.modality = 'RTSTRUCT'
        WHERE plan.modality = 'RTPLAN'
        """
        + _NOT_SENT.format(alias="plan", mod="RTPLAN"),
        ["rtplan_sop_uid", "patient_id", "study_uid", "rtstruct_sop_uid", "ct_series_uid"],
    ),
    "RTSTRUCT": (
        """
        SELECT struct.sop_instance_uid, struct.patient_id, struct.study_instance_uid,
               struct.referenced_ct_series_uid
        FROM dicom_insert struct
        WHERE struct.modality = 'RTSTRUCT'
        """
        + _NOT_SENT.format(alias="struct", mod="RTSTRUCT"),
        ["rtstruct_sop_uid", "patient_id", "study_uid", "ct_series_uid"],
    ),
}


class ModalityRequest(BaseModel):
    modality: str


app = FastAPI()

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


@app.post("/sop_instance_uids")
async def get_new_sop_instance_uids(request: ModalityRequest):
    modality = request.modality

    try:
        sql_query = """
        SELECT sop_instance_uid, study_instance_uid, patient_name
        FROM dicom_insert
        WHERE modality = %s
        AND sop_instance_uid NOT IN (
            SELECT sop_instance_uid
            FROM calculation_status
            WHERE status = TRUE AND modality = %s
        )
        """
        results = db.fetch_all(sql_query, (modality, modality))

        new_sops = (
            [{"sop_instance_uid": row[0], "study_instance_uid": row[1], "patient_name": row[2]} for row in results]
            if results
            else []
        )

        for sop in new_sops:
            db.execute_query(
                """
                INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp)
                VALUES (%s, %s, TRUE, %s)
                """,
                (sop["sop_instance_uid"], modality, datetime.now(UTC)),
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    else:
        return {"modality": modality, "new_sop_instances": new_sops}


@app.post("/rt_package")
async def get_rt_package(request: ModalityRequest):
    modality = request.modality.upper()

    if modality not in _RT_CHAIN_CONFIG:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported modality '{modality}'. Must be one of: {sorted(_RT_CHAIN_CONFIG)}",
        )

    sql, columns = _RT_CHAIN_CONFIG[modality]
    anchor_col = columns[0]

    try:
        rows = db.fetch_all(sql, ())
        packages = [dict(zip(columns, row, strict=False)) for row in rows]
        for pkg in packages:
            db.execute_query(
                "INSERT INTO calculation_status (sop_instance_uid, modality, status, timestamp) VALUES (%s, %s, TRUE, %s)",
                (pkg[anchor_col], modality, datetime.now(UTC)),
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    else:
        return {"modality": modality, "packages": packages}
