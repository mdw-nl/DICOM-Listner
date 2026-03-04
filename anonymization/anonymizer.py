import logging
import pandas as pd
import json
import os
import sys
import hashlib
import pydicom
import re
import yaml
from datetime import datetime
from deid.dicom import get_files, replace_identifiers, get_identifiers
from deid.config import DeidRecipe
from pydicom.datadict import add_private_dict_entries
import tempfile
import contextlib
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


@contextlib.contextmanager
def suppress_output():
    old_out, old_err = sys.stdout, sys.stderr
    devnull_out = open(os.devnull, "w")
    devnull_err = open(os.devnull, "w")
    try:
        sys.stdout, sys.stderr = devnull_out, devnull_err
        yield
    finally:
        sys.stdout, sys.stderr = old_out, old_err
        devnull_out.close()
        devnull_err.close()


class Anonymizer:

    def __init__(self, path_files="dicomsorter/dicomsorter/recipes/"):
        # Get the private tags from the varaibles.yaml file
        path_var = os.path.join(path_files, "variables.yaml")
        with open(path_var, 'r') as f:
            config_data = yaml.safe_load(f)

        variables = config_data.get("variables", {})
        self.PatientName = variables.get("PatientName")
        self.ProfileName = variables.get("ProfileName")
        self.ProjectName = variables.get("ProjectName")
        self.TrialName = variables.get("TrialName")
        self.SiteName = variables.get("SiteName")
        self.SiteID = variables.get("SiteID")

        # Paths to the recipes that are mounted in the digione infrastructure docker compose volumes.
        self.recipe_path = os.path.join(path_files, "recipe.dicom")

        self.patient_lookup_csv = os.path.join(path_files, "patient_lookup.csv")
        df = pd.read_csv(self.patient_lookup_csv, dtype=str)
        self._patient_map = dict(zip(df["original"], df["new"]))

        self.ROI_normalization_path = os.path.join(path_files, "ROI_normalization.yaml")

        with open(self.ROI_normalization_path, "r") as f:
            roi_map = yaml.safe_load(f) or {}
        self._compiled_roi_map = {
            canonical: [re.compile(p, re.IGNORECASE) for p in patterns]
            for canonical, patterns in roi_map.items()
        }

        # ✅ Load recipe ONCE
        self._recipe = DeidRecipe(deid=self.recipe_path)

        # ✅ Register private tag dict entries ONCE (global)
        private_entries = {
            0x10011001: ("SH", "1", "ProfileName"),
            0x10031001: ("SH", "1", "ProjectName"),
            0x10051001: ("SH", "1", "TrialName"),
            0x10071001: ("SH", "1", "SiteName"),
            0x10091001: ("SH", "1", "SiteID"),
        }
        add_private_dict_entries("Deid", private_entries)

    @staticmethod
    def hash_func(item, value, field, dicom):
        return hashlib.md5(value.encode()).hexdigest()[:16]

    @staticmethod
    def patient_mapping(csv_path):
        df = pd.read_csv(csv_path)

        def lookup(item, value, field, dicom):
            patient_id = dicom.PatientID
            matched = df.loc[df['original'] == patient_id, 'new']
            if matched.empty:
                raise ValueError(
                    f"PatientID: '{patient_id}' not found in patient lookup CSV. Stopping the pipeline for this patient")
            return matched.values[0]

        return lookup

    @staticmethod
    def current_date(field, value, item, dicom):
        now = datetime.now()
        return f"deid: {now.strftime('%d%m%Y:%H%M%S')}"



    def csv_lookup_func(self, item, value, field, dicom):
        patient_id = getattr(dicom, "PatientID", None)
        if patient_id is None:
            raise ValueError("PatientID missing")
        try:
            return self._patient_map[patient_id]
        except KeyError:
            raise ValueError(f"PatientID '{patient_id}' not found in patient lookup CSV")

    def ROI_normalization(self, rtstruct):
        """
        Normalize ROI names in all RTSTRUCT files in the folder using the YAML mapping.
        """

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
                logging.warning(f"No ROI map found for '{original_raw}' in RTSTRUCT dataset")

        return rtstruct

    def is_patient_known(self, patient_id: str) -> bool:
        return patient_id in self._patient_map

    def anonymize(self, dicom_obj):

        # Suppress logger output during deid processing
        with suppress_output():
            # Create a temporary folder for the DICOM file because deid does not work for in memory processing
            with tempfile.TemporaryDirectory() as tmpdir:
                temp_path = os.path.join(tmpdir, "temp.dcm")

                # Save the in-memory dataset to a temporary file
                dicom_obj.save_as(temp_path, write_like_original=False)

                del dicom_obj

                items = get_identifiers([temp_path], expand_sequences=False)

                for key in items:
                    items[key].update({
                        "CSV_lookup_func": self.csv_lookup_func,
                        "hash_func": self.hash_func,
                        "DeIdentificationMethod": self.current_date,
                        "PatientName": self.PatientName
                    })

                # Apply anonymization in-place on the temp file
                # recipe = DeidRecipe(deid=recipe_path)
                # updated = replace_identifiers(dicom_files=[temp_path], deid=recipe, ids=items)
                updated = replace_identifiers(dicom_files=[temp_path], deid=self._recipe, ids=items)
                
                del items
                
                dicom_obj = updated[0]

                del updated

        # Add private tags definitions
        # private_entries = {
        #    0x10011001: ("SH", "1", "ProfileName"),
        #    0x10031001: ("SH", "1", "ProjectName"),
        #    0x10051001: ("SH", "1", "TrialName"),
        #    0x10071001: ("SH", "1", "SiteName"),
        #    0x10091001: ("SH", "1", "SiteID"),
        # }
        # add_private_dict_entries("Deid", private_entries)

        # Update private blocks
        dicom_obj.remove_private_tags()
        dicom_obj.private_block(0x1001, 'Deid', create=True).add_new(0x01, "SH", self.ProfileName)
        dicom_obj.private_block(0x1003, 'Deid', create=True).add_new(0x01, "SH", self.ProjectName)
        dicom_obj.private_block(0x1005, 'Deid', create=True).add_new(0x01, "SH", self.TrialName)
        dicom_obj.private_block(0x1007, 'Deid', create=True).add_new(0x01, "SH", self.SiteName)
        dicom_obj.private_block(0x1009, 'Deid', create=True).add_new(0x01, "SH", self.SiteID)
        
        gc.collect()

        return dicom_obj

    def run(self, dicomdata):
        try:
            if dicomdata.Modality == "RTSTRUCT":
                self.ROI_normalization(dicomdata)

            dicomdata = self.anonymize(dicomdata)
            logger.info("Anonymization process completed.")
            logger.debug(f"Anonymized DICOM data: {dicomdata}")
            if dicomdata is None:
                logging.error("Anonymization failed, returning None")
                return None

            logging.info("File anonymised successfully")
            return dicomdata

        except Exception as e:
            logger.error(f"Error processing message in run(): {e}")
            return None
