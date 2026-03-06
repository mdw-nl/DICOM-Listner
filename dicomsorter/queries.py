CREATE_DATABASE_QUERY = """
CREATE TABLE IF NOT EXISTS dicom_insert (
    id SERIAL  PRIMARY KEY ,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    patient_name TEXT NOT NULL,
    patient_id TEXT NOT NULL,
    study_instance_uid TEXT NOT NULL,
    series_instance_uid TEXT NOT NULL,
    modality TEXT NOT NULL,
    sop_instance_uid TEXT NOT NULL UNIQUE,
    sop_class_uid TEXT NOT NULL,
    instance_number TEXT,
    file_path TEXT,
    referenced_sop_class_uid TEXT,
    referenced_rt_plan_uid TEXT,
    referenced_rtstruct_sop_uid TEXT,
    referenced_ct_series_uid TEXT,
    modality_type TEXT,
    assoc_id TEXT
);
"""

MIGRATE_ADD_RTSTRUCT_REF = """
ALTER TABLE dicom_insert ADD COLUMN IF NOT EXISTS referenced_rtstruct_sop_uid TEXT;
"""

MIGRATE_ADD_CT_SERIES_REF = """
ALTER TABLE dicom_insert ADD COLUMN IF NOT EXISTS referenced_ct_series_uid TEXT;
"""

CREATE_DATABASE_QUERY_2 = """
CREATE TABLE IF NOT EXISTS associations (
    uuid TEXT PRIMARY KEY,
    ae_title TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    port INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL
);"""

CREATE_DATABASE_QUERY_3 = """
CREATE TABLE IF NOT EXISTS calculation_status (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sop_instance_uid TEXT,
    modality TEXT NOT NULL,
    status BOOLEAN NOT NULL,
    timestamp TIMESTAMP NOT NULL
);"""

MIGRATE_CALC_STATUS_SOP_UID = """
ALTER TABLE calculation_status ADD COLUMN IF NOT EXISTS sop_instance_uid TEXT;
"""

MIGRATE_CALC_STATUS_MODALITY = """
ALTER TABLE calculation_status ADD COLUMN IF NOT EXISTS modality TEXT;
"""


INSERT_QUERY_DICOM_META = """
    INSERT INTO dicom_insert (
        patient_name, patient_id, study_instance_uid, series_instance_uid, modality,
        sop_instance_uid, sop_class_uid, instance_number,
        file_path, referenced_rt_plan_uid, referenced_sop_class_uid,
        referenced_rtstruct_sop_uid, referenced_ct_series_uid,
        modality_type, assoc_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


INSERT_QUERY_DICOM_ASS = """
    INSERT INTO associations (
        uuid, ae_title, ip_address, port, timestamp
    ) VALUES (%s, %s, %s, %s, %s)
"""

UNIQUE_UID_SELECT = """
    SELECT EXIST(SELECT study_instance_uid FROM public.dicom_insert WHERE study_instance_uid = %s;)
    VALUES (%s);
"""

CREATE_DVH_RESULT = """
CREATE TABLE IF NOT EXISTS dvh_result (
    result_id SERIAL PRIMARY KEY,
    json_id TEXT UNIQUE NOT NULL,
    dose_bins DOUBLE PRECISION[] NOT NULL,
    volume_bins DOUBLE PRECISION[] NOT NULL,
    "D2" DOUBLE PRECISION,
    "D50" DOUBLE PRECISION,
    "D95" DOUBLE PRECISION,
    "D98" DOUBLE PRECISION,
    min_dose DOUBLE PRECISION,
    mean_dose DOUBLE PRECISION,
    max_dose DOUBLE PRECISION,
    "V0" DOUBLE PRECISION,
    "V15" DOUBLE PRECISION,
    "V35" DOUBLE PRECISION
);
"""

CREATE_DVH_PACKAGE = """
CREATE TABLE IF NOT EXISTS dvh_package (
    sop_instance_uid TEXT NOT NULL,
    roi_name TEXT NOT NULL,
    result_id INTEGER NOT NULL REFERENCES dvh_result(result_id) ON DELETE CASCADE
);
"""

CREATE_PATIENT_ID_MAP = """
CREATE TABLE IF NOT EXISTS patient_id_map (
    id SERIAL PRIMARY KEY,
    original_patient_id TEXT UNIQUE NOT NULL,
    generated_patient_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""

CREATE_PACS_ARCHIVE = """
CREATE TABLE IF NOT EXISTS pacs_archive (
    sop_instance_uid TEXT PRIMARY KEY,
    series_instance_uid TEXT NOT NULL,
    modality TEXT NOT NULL,
    study_instance_uid TEXT NOT NULL,
    patient_id TEXT NOT NULL,
    queued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'pending'
);"""

INSERT_PACS_ARCHIVE_PENDING = """
INSERT INTO pacs_archive (sop_instance_uid, series_instance_uid, modality, study_instance_uid, patient_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (sop_instance_uid) DO NOTHING;
"""

UPDATE_PACS_ARCHIVE_ARCHIVED = """
UPDATE pacs_archive SET status = 'archived', archived_at = NOW()
WHERE sop_instance_uid = %s;
"""

QUERY_PENDING_SOPS = """
SELECT di.sop_instance_uid, di.series_instance_uid, di.modality, di.patient_id, di.file_path
FROM dicom_insert di
LEFT JOIN pacs_archive pa
    ON pa.sop_instance_uid = di.sop_instance_uid AND pa.status = 'archived'
WHERE di.study_instance_uid = %s AND pa.sop_instance_uid IS NULL;
"""

TABLES = [
    ("dicom_insert", CREATE_DATABASE_QUERY),
    ("associations", CREATE_DATABASE_QUERY_2),
    ("calculation_status", CREATE_DATABASE_QUERY_3),
    ("patient_id_map", CREATE_PATIENT_ID_MAP),
    ("dvh_result", CREATE_DVH_RESULT),
    ("dvh_package", CREATE_DVH_PACKAGE),
    ("pacs_archive", CREATE_PACS_ARCHIVE),
]

MIGRATIONS = [
    MIGRATE_ADD_RTSTRUCT_REF,
    MIGRATE_ADD_CT_SERIES_REF,
    MIGRATE_CALC_STATUS_SOP_UID,
    MIGRATE_CALC_STATUS_MODALITY,
]
