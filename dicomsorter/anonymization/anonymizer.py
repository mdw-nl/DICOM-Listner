import contextlib
import gc
import hashlib
import logging
import os
import re
import tempfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from deid.config import DeidRecipe
from deid.dicom import get_identifiers, replace_identifiers
from pydicom.datadict import add_private_dict_entries

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def suppress_output() -> Generator[None, None, None]:
    devnull = Path(os.devnull)
    with (
        devnull.open("w") as out,
        devnull.open("w") as err,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        yield


class Anonymizer:
    def __init__(self, path_files="dicomsorter/dicomsorter/recipes/"):
        path_base = Path(path_files)
        path_var = path_base / "variables.yaml"
        with path_var.open() as f:
            config_data = yaml.safe_load(f)

        variables = config_data.get("variables", {})
        self.PatientName = variables.get("PatientName")
        self.ProfileName = variables.get("ProfileName")
        self.ProjectName = variables.get("ProjectName")
        self.TrialName = variables.get("TrialName")
        self.SiteName = variables.get("SiteName")
        self.SiteID = variables.get("SiteID")

        self.recipe_path = str(path_base / "recipe.dicom")
        self.patient_lookup_csv = str(path_base / "patient_lookup.csv")
        df = pd.read_csv(self.patient_lookup_csv, dtype=str)
        self._patient_map = dict(zip(df["original"], df["new"], strict=False))

        self.ROI_normalization_path = str(path_base / "ROI_normalization.yaml")
        with Path(self.ROI_normalization_path).open() as f:
            roi_map = yaml.safe_load(f) or {}
        self._compiled_roi_map = {
            canonical: [re.compile(p, re.IGNORECASE) for p in patterns] for canonical, patterns in roi_map.items()
        }

        self._recipe = DeidRecipe(deid=self.recipe_path)

        private_entries = {
            0x10011001: ("SH", "1", "ProfileName"),
            0x10031001: ("SH", "1", "ProjectName"),
            0x10051001: ("SH", "1", "TrialName"),
            0x10071001: ("SH", "1", "SiteName"),
            0x10091001: ("SH", "1", "SiteID"),
        }
        add_private_dict_entries("Deid", private_entries)

    @staticmethod
    def hash_func(item, value, field, dicom):  # noqa: ARG004
        return hashlib.md5(value.encode()).hexdigest()[:16]  # noqa: S324

    @staticmethod
    def patient_mapping(csv_path):
        df = pd.read_csv(csv_path)

        def lookup(item, value, field, dicom):
            patient_id = dicom.PatientID
            matched = df.loc[df["original"] == patient_id, "new"]
            if matched.empty:
                raise ValueError(
                    f"PatientID: '{patient_id}' not found in patient lookup CSV. Stopping the pipeline for this patient"
                )
            return matched.to_numpy()[0]

        return lookup

    @staticmethod
    def current_date(field, value, item, dicom):  # noqa: ARG004
        now = datetime.now()
        return f"deid: {now.strftime('%d%m%Y:%H%M%S')}"

    def csv_lookup_func(self, item, value, field, dicom):  # noqa: ARG002
        patient_id = getattr(dicom, "PatientID", None)
        if patient_id is None:
            raise ValueError("PatientID missing")
        try:
            return self._patient_map[patient_id]
        except KeyError as e:
            raise ValueError(f"PatientID '{patient_id}' not found in patient lookup CSV") from e

    def ROI_normalization(self, rtstruct):
        """Normalize ROI names in all RTSTRUCT files in the folder using the YAML mapping."""
        compiled_map = self._compiled_roi_map

        for roi in rtstruct.StructureSetROISequence:
            original_raw = roi.ROIName
            original = original_raw.strip()

            normalized = None
            for canonical, regex_list in compiled_map.items():
                if any(regex.search(original) for regex in regex_list):
                    normalized = canonical
                    break

            if normalized and original_raw != normalized:
                roi.ROIName = normalized
            elif normalized is None:
                logger.warning("No ROI map found for '%s' in RTSTRUCT dataset", original_raw)

        return rtstruct

    def is_patient_known(self, patient_id: str) -> bool:
        return patient_id in self._patient_map

    def anonymize(self, dicom_obj):
        with suppress_output(), tempfile.TemporaryDirectory() as tmpdir:
            temp_path = str(Path(tmpdir) / "temp.dcm")

            dicom_obj.save_as(temp_path, write_like_original=False)
            del dicom_obj

            items = get_identifiers([temp_path], expand_sequences=False)

            for key in items:
                items[key].update(
                    {
                        "CSV_lookup_func": self.csv_lookup_func,
                        "hash_func": self.hash_func,
                        "DeIdentificationMethod": self.current_date,
                        "PatientName": self.PatientName,
                    }
                )

            updated = replace_identifiers(dicom_files=[temp_path], deid=self._recipe, ids=items)

            del items

            dicom_obj = updated[0]
            del updated

        dicom_obj.remove_private_tags()
        dicom_obj.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        dicom_obj.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        dicom_obj.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        dicom_obj.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        dicom_obj.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)

        gc.collect()

        return dicom_obj

    def run(self, dicomdata):
        try:
            if dicomdata.Modality == "RTSTRUCT":
                self.ROI_normalization(dicomdata)

            dicomdata = self.anonymize(dicomdata)
            logger.info("Anonymization process completed.")
            logger.debug("Anonymized DICOM data: %s", dicomdata)
            if dicomdata is None:
                logger.error("Anonymization failed, returning None")
                return None
        except Exception:
            logger.exception("Error processing message in run()")
            return None
        else:
            logger.info("File anonymised successfully")
            return dicomdata
