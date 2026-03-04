from pathlib import Path

from pydicom import Dataset
from pydicom.valuerep import PersonName

from dicomsorter.src.global_var import BASE_DIR


def return_dicom_data(ds: Dataset):
    """:param ds:
    :return:
    """
    patient_name = ds.PatientName if "PatientName" in ds else "UNKNOWN"
    if isinstance(patient_name, PersonName):
        patient_name = str(patient_name)

    patient_id = ds.PatientID if "PatientID" in ds else "UNKNOWN"
    study_uid = ds.StudyInstanceUID if "StudyInstanceUID" in ds else "UNKNOWN"
    series_uid = ds.SeriesInstanceUID if "SeriesInstanceUID" in ds else "UNKNOWN"
    modality = ds.Modality if "Modality" in ds else "UNKNOWN"
    sop_uid = ds.SOPInstanceUID if "SOPInstanceUID" in ds else "UNKNOWN"
    sop_class_uid = ds.SOPClassUID if "SOPClassUID" in ds else "UNKNOWN"
    instance_number = ds.InstanceNumber if "InstanceNumber" in ds else "UNKNOWN"
    instance_number = "UNKNOWN" if instance_number is None or instance_number == "UNKNOWN" else int(instance_number)
    modality_type = ds.get("ModalityType", "UNKNOWN")
    referenced_rt_plan_seq = ds.get("ReferencedRTPlanSequence", [{}])
    referenced_rt_plan_uid = (
        referenced_rt_plan_seq[0].get("ReferencedSOPInstanceUID", "UNKNOWN") if referenced_rt_plan_seq else "UNKNOWN"
    )
    referenced_sop_class_uid = (
        referenced_rt_plan_seq[0].get("ReferencedSOPClassUID", "UNKNOWN") if referenced_rt_plan_seq else "UNKNOWN"
    )

    return (
        patient_name,
        patient_id,
        study_uid,
        series_uid,
        modality,
        sop_uid,
        sop_class_uid,
        instance_number,
        modality_type,
        referenced_rt_plan_uid,
        referenced_sop_class_uid,
    )


def create_folder(patient_id, study_uid, modality, sop_uid):
    """:param patient_id:
    :param study_uid:
    :param modality:
    :param sop_uid:
    :return:
    """
    patient_folder = Path(BASE_DIR) / patient_id / study_uid / modality
    patient_folder.mkdir(parents=True, exist_ok=True)
    return str(patient_folder / f"{sop_uid}.dcm")
