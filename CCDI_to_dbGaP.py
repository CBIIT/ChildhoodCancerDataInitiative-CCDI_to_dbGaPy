"""
This script is tool in Python that takes validated CCDI manifest and
a previous submission (optional), and creates submission
files for dbGaP.

Authors: Qiong Liu <qiong.liu@nih.gov>
"""
import argparse
from typing import List, Dict, Tuple, TypeVar
import warnings
import logging
from datetime import date
import pandas as pd
import sys
import os
import json
from pathlib import Path


ExcelReader = TypeVar("ExcelReader")
DataFrame = TypeVar("DataFrame")
ExcelFile = TypeVar("ExcelFile")
Series = TypeVar("Series")


class Color:
    """A class for terminal color codes."""

    BOLD = "\033[1m"
    BLUE = "\033[94m"
    WHITE = "\033[97m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD_WHITE = BOLD + WHITE
    BOLD_BLUE = BOLD + BLUE
    BOLD_GREEN = BOLD + GREEN
    BOLD_YELLOW = BOLD + YELLOW
    BOLD_RED = BOLD + RED
    END = "\033[0m"


class ColorLogFormatter(logging.Formatter):
    """A class for formatting colored logs."""

    FORMAT = "%(asctime)s - %(prefix)s%(levelname)s%(suffix)s - %(message)s"

    LOG_LEVEL_COLOR = {
        "DEBUG": {"prefix": "", "suffix": ""},
        "INFO": {"prefix": Color.GREEN, "suffix": Color.END},
        "WARNING": {"prefix": Color.YELLOW, "suffix": Color.END},
        "ERROR": {"prefix": Color.RED, "suffix": Color.END},
        "CRITICAL": {"prefix": Color.BOLD_RED, "suffix": Color.END},
    }

    def format(self, record):
        """Format log records with a default prefix and suffix to terminal color codes that corresponds to the log level name."""
        if not hasattr(record, "prefix"):
            record.prefix = self.LOG_LEVEL_COLOR.get(record.levelname.upper()).get(
                "prefix"
            )

        if not hasattr(record, "suffix"):
            record.suffix = self.LOG_LEVEL_COLOR.get(record.levelname.upper()).get(
                "suffix"
            )

        formatter = logging.Formatter(self.FORMAT, "%H:%M:%S")
        return formatter.format(record)


def get_date() -> str:
    """Returns the current date while the script is running"""
    date_obj = date.today()
    return date_obj.isoformat()


def get_logger(loggername: str, log_level: str):
    """Returns a basic logger with a logger name using a std format

    log level can be set using one of the values in log_levels.
    """
    log_levels = {  # sorted level
        "notset": logging.NOTSET,  # 00
        "debug": logging.DEBUG,  # 10
        "info": logging.INFO,  # 20
        "warning": logging.WARNING,  # 30
        "error": logging.ERROR,  # 40
    }

    logger_filename = loggername + "_" + get_date() + ".log"
    logger = logging.getLogger(loggername)
    logger.setLevel(log_levels[log_level])

    # set the stream handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(ColorLogFormatter())
    stream_handler.setLevel(log_levels["info"])

    # set the file handler
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(logger_filename, mode="w")
    file_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def excel_sheets_to_dict(excel_file: ExcelFile, no_names: List) -> Dict:
    """Returns a list of sheet names in the excel file input"""
    warnings.simplefilter(action="ignore", category=UserWarning)
    sheetnames = excel_file.sheet_names
    sheetnames_subset = [i for i in sheetnames if i not in no_names]
    excel_dict = {}
    for i in sheetnames_subset:
        i_df = pd.read_excel(excel_file, sheet_name=i, dtype=str)
        excel_dict[i] = i_df
    excel_file.close()
    return excel_dict


def ccdi_manifest_to_dict(excel_file: ExcelFile) -> Dict:
    """Reads a validated CDDI manifest excel and retruns
    a dictionary with sheetnames as keys and pandas
    dataframes as values

    """
    sheets_to_avoid = ["README and INSTRUCTIONS", "Dictionary", "Terms and Value Sets"]
    ccdi_dict_raw = excel_sheets_to_dict(excel_file, no_names=sheets_to_avoid)
    ccdi_dict = {}
    for key, item_df in ccdi_dict_raw.items():
        # drop the column "type" from data frame
        item_df = item_df.drop(["type"], axis=1)
        # remove any line or column that has all na values
        item_df.dropna(axis=0, how="all", inplace=True)
        # keep empty columnsat this step
        # item_df.dropna(axis=1, how="all", inplace=True)

        # some more filtering criteria
        # test if the df is empty
        # test if all column names contain a '.', if yes, do not add it to dict
        item_df_names = item_df.columns
        if len([j for j in item_df_names if "." in j]) != len(item_df_names):
            ccdi_dict[key] = item_df
        else:
            pass
    del ccdi_dict_raw
    return ccdi_dict


def load_args():
    """Returns args for arguments
    """
    # set up arguments for this script
    parser = argparse.ArgumentParser(
        description="This script is a python version to generate dbGaP submission files using a validated CCDI submission manifest"
    )
    parser._action_groups.pop()
    required_arg = parser.add_argument_group("required arguments")
    optional_arg = parser.add_argument_group("optional arguments")
    required_arg.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="A validated dataset file  based on the template CCDI_submission_metadata_template (.xlsx)",
    )
    optional_arg.add_argument(
        "-s",
        "--previous_submission",
        type=str,
        required=False,
        help="A previous dbGaP submission directory for the same phs_id study.",
    )

    args = parser.parse_args()
    return args


def check_participant_unique(sub_df: DataFrame, logger) -> None:
    """Checks if any subject_ID appears in multiple rows
    """
    sub_df_size = sub_df.groupby("SUBJECT_ID").size()
    if sub_df_size.max() > 1:
        subject_warning = sub_df_size[sub_df_size > 1].index.tolist()
        logger.warning(
            f"Participants with more than one record were found:\n{*subject_warning,}"
        )
    else:
        pass


class DD_dataframe:
    """A class helps to create 3 dataframes of data dictionary (DD) for
    Subject consent, subject sample, and sample tummor status 
    """
    def __init__(self) -> None:
        self.subject_consent_dd = {
            "VARNAME": ["VARDESC", "TYPE", "VALUES"],
            "SUBJECT_ID": ["Subject ID", "string"],
            "CONSENT": [
                "Consent group as determined by DAC",
                "encoded value",
                "1=General Research Use (GRU)",
            ],
            "SEX": [
                "Biological sex",
                "encoded value",
                "1=Male",
                "2=Female",
                "UNK=Unknown",
            ],
        }
        self.subject_sample_dd = {
            "VARNAME": ["VARDESC", "TYPE", "VALUES"],
            "SUBJECT_ID": ["Subject ID", "string"],
            "SAMPLE_ID": ["Sample ID", "string"],
        }
        self.sample_tumor_dd = {
            "VARNAME": ["VARDESC", "TYPE", "VALUES"],
            "SAMPLE_ID": ["Sample ID", "string"],
            "SAMPLE_TUMOR_STATUS": ["Sample Tumor Status", "Status"],
        }

    @classmethod
    def create_dd_df(self, dd_dict: Dict) -> DataFrame:
        df = pd.DataFrame(
            dict([(key, pd.Series(value)) for key, value in dd_dict.items()])
        ).transpose()
        df = df.reset_index()
        return df

    def create_dd_all(self) -> Tuple:
        subject_consent_dd_output = self.create_dd_df(self.subject_consent_dd)
        subject_sample_dd_output = self.create_dd_df(self.subject_sample_dd)
        sample_tumor_dd_output = self.create_dd_df(self.sample_tumor_dd)
        return (
            subject_consent_dd_output,
            subject_sample_dd_output,
            sample_tumor_dd_output,
        )


class Pre_dbGaP_combine:
    """A class that concates previous submission to current submission
    """
    def __init__(
        self,
        pre_sub_dir: List,
        subject_consent: DataFrame,
        subject_sample: DataFrame,
        sample_tumor: DataFrame,
        logger,
    ) -> None:
        self.pre_sub_dir = pre_sub_dir
        self.subject_consent = subject_consent
        self.subject_sample = subject_sample
        self.sample_tumor = sample_tumor
        self.logger = logger

    def read_pre_dir(self):
        item_list = [i for i in os.listdir(self.pre_sub_dir) if "txt" in i]
        pre_subject_consent = [k for k in item_list if "SC_DS_" in k][0]
        pre_subject_sample = [j for j in item_list if "SSM_DS_" in j][0]
        pre_sample_tumor = [l for l in item_list if "SA_DS_" in l][0]
        self.logger.info(
            f"Previous dbGaP submission files were found:\n{pre_subject_consent}\n{pre_subject_sample}\n{pre_sample_tumor}"
        )

        pre_subject_consent_df = pd.read_csv(
            os.path.join(self.pre_sub_dir, pre_subject_consent), sep="\t", header=0
        )
        pre_subject_sample_df = pd.read_csv(
            os.path.join(self.pre_sub_dir, pre_subject_sample), sep="\t", header=0
        )
        pre_sample_tumor_df = pd.read_csv(
            os.path.join(self.pre_sub_dir, pre_sample_tumor), sep="\t", header=0
        )

        combined_subject_consent = pd.concat(
            [pre_subject_consent_df, self.subject_consent], ignore_index=True
        ).drop_duplicates()
        combined_subject_sample = pd.concat(
            [pre_subject_sample_df, self.subject_sample], ignore_index=True
        ).drop_duplicates()
        combined_sample_tumor = pd.concat(
            [pre_sample_tumor_df, self.sample_tumor], ignore_index=True
        ).drop_duplicates()

        return combined_subject_consent, combined_subject_sample, combined_sample_tumor


def create_meta_json(phs_id: str) -> Dict:
    """Returns a metadata.json describing all 6 submission files
    """
    dict_name = phs_id + "_" + get_date()
    file_name_pattern = phs_id + "_dbGaP_submission.txt"
    sc_ds_filename = "SC_DS_" + file_name_pattern
    sa_ds_filename = "SA_DS_" + file_name_pattern
    ssm_ds_filename = "SSM_DS_" + file_name_pattern

    dict_files = []
    dict_files.append({"name": sc_ds_filename, "type": "subject_consent_file"})
    dict_files.append(
        {"name": "SC_DD.xlsx", "type": "subject_consent_data_dictionary_file"}
    )
    dict_files.append({"name": sa_ds_filename, "type": "sample_attributes"})
    dict_files.append({"name": "SA_DD.xlsx", "type": "sample_attributes_dd"})
    dict_files.append({"name": ssm_ds_filename, "type": "subject_sample_mapping_file"})
    dict_files.append(
        {"name": "SSM_DD.xlsx", "type": "subject_sample_mapping_data_dictionary_file"}
    )
    return_dict = {
        "NAME": dict_name,
        "FILES": dict_files,
    }
    return return_dict


def main():
    args = load_args()

    manifest = args.file

    # Create logger instance
    logger = get_logger(loggername="CCDI_to_dbGaP", log_level="info")
    logger.warning(
        "THIS SCRIPT IS ONLY MEANT FOR CCDI AND ALL CONSENT IS ASSUMED TO BE GRU, CONSENT GROUP 1."
    )

    # Read the content in CCDI manifest
    try:
        manifest_f = pd.ExcelFile(manifest)
        logger.info(f"Checking file {manifest}")
        # create a dict using the CCDI manifest
        workbook_dict = ccdi_manifest_to_dict(manifest_f)
        logger.info(f"Reading the validated CCDI manifest {manifest}")
    except FileNotFoundError as err:
        logger.error(err)
        sys.exit()
    except ValueError as err:
        logger.error(err)
        sys.exit()
    except:
        logger.error(f"Issue occurred while openning file {manifest}")
        sys.exit()

    # extract particpant and sample sheet
    participant_df = workbook_dict["participant"]
    sample_df = workbook_dict["sample"]

    # subject_consent
    subject_consent = participant_df[["participant_id", "sex_at_birth"]].rename(
        columns={"participant_id": "SUBJECT_ID", "sex_at_birth": "SEX"}
    )
    subject_consent["CONSENT"] = "1"
    subject_consent["SEX"][subject_consent["SEX"].str.contains("Female")] = "2"
    subject_consent["SEX"][subject_consent["SEX"].str.contains("Male")] = "1"
    subject_consent["SEX"][~subject_consent["SEX"].str.contains("1|2")] = "UNK"
    # drop rows with empty SUBJECT_ID and drop duplicates
    subject_consent = (
        subject_consent.dropna(subset=["SUBJECT_ID"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # check if each participant only appears in one row
    check_participant_unique(sub_df=subject_consent, logger=logger)

    # subject_sample
    subject_sample = sample_df[["participant.participant_id", "sample_id"]].rename(
        columns={"participant.participant_id": "SUBJECT_ID", "sample_id": "SAMPLE_ID"}
    )
    subject_sample = (
        subject_sample.dropna(subset=["SUBJECT_ID", "SAMPLE_ID"], how="any")
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # sample_tumor
    sample_tumor = sample_df[["sample_id", "sample_tumor_status"]].rename(
        columns={"sample_id": "SAMPLE_ID", "sample_tumor_status": "SAMPLE_TUMOR_STATUS"}
    )
    sample_tumor = (
        sample_tumor.dropna(subset=["SAMPLE_ID"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Create DD dataframes
    (
        subject_consent_dd_df,
        subject_sample_dd_df,
        sample_tumor_dd_df,
    ) = DD_dataframe().create_dd_all()

    if args.previous_submission:
        try:
            # look for files with txt extension
            (
                subject_consent,
                subject_sample,
                sample_tumor,
            ) = Pre_dbGaP_combine(
                pre_sub_dir=args.previous_submission,
                subject_consent=subject_consent,
                subject_sample=subject_sample,
                sample_tumor=sample_tumor,
                logger=logger,
            ).read_pre_dir()
        except FileNotFoundError:
            logger.error(f"Directory {args.previous_submission} does not exit")
        except PermissionError:
            logger.error(f"Permission denied for directory {args.previous_submission}")
    else:
        logger.warning("No previous submission directory was provided")

    # prepare meta json output
    phs_id = participant_df["study.study_id"][0]
    meta_dict = create_meta_json(phs_id)

    # create output directory
    output_dir_path = os.path.join(
        os.getcwd(), phs_id + "_dbGaP_submission_" + get_date()
    )
    Path(output_dir_path).mkdir(parents=True, exist_ok=True)
    logger.info(f"Created an output folder if not exist at {output_dir_path}")

    # write dd files
    subject_consent_dd_df.to_excel(
        os.path.join(output_dir_path, "SC_DD.xlsx"), header=False, index=False
    )
    subject_sample_dd_df.to_excel(
        os.path.join(output_dir_path, "SSM_DD.xlsx"), header=False, index=False
    )
    sample_tumor_dd_df.to_excel(
        os.path.join(output_dir_path, "SA_DD.xlsx"), header=False, index=False
    )
    logger.info("Writing 3 DD files")

    # write txt files
    subject_consent.to_csv(
        os.path.join(output_dir_path, "SC_DS_" + phs_id + "_dbGaP_submission.txt"),
        sep="\t",
        index=False,
    )
    subject_sample.to_csv(
        os.path.join(output_dir_path, "SSM_DS_" + phs_id + "_dbGaP_submission.txt"),
        sep="\t",
        index=False,
    )
    sample_tumor.to_csv(
        os.path.join(output_dir_path, "SA_DS_" + phs_id + "_dbGaP_submission.txt"),
        sep="\t",
        index=False,
    )
    logger.info("Writing SC_DS, SSM_DS, SA_DS files")

    # write meta json to file
    with open(os.path.join(output_dir_path, "metadata.json"), "w") as fp:
        json.dump(meta_dict, fp)
    logger.info("Writing metadata.json")

    logger.info("Script finishe!")


if __name__ == "__main__":
    main()
