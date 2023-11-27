# ChildhoodCancerDataInitiative-CCDI_to_dbGaPy

This repo contains a python script which takes data from a validated CCDI submission manifest and creates dbGaP submission files specifically for a CCDI project.

## Table of Contents
- [Python environment management](#python-environment-management)
- [Usage instruction](#usage-instruction)

---
### Python environment management
A controlled **virtual environment** of Python is always recommanded for running any python package/script due to dependency management purpose. There are many tools that you can use to create a virtual environment, such as `pyenv`, `virtualenv` or `conda`. An instruction is included here on how to create a `conda env` with all the dependencies installed.

- **Conda install**

    Conda is an open source package management system and environment management system that runs on Windows, macOs, and Lunix. [Here](https://docs.conda.io/projects/miniconda/en/latest/) is the site of installation instruction. Please pick the right package based on your operation system.

- **Create a conda env**

    An environment yaml `conda_environment.yml` can be be found under folder `envs/`. To create the environment, simply run

    ```bash
    conda env create -f <path_to_env_yml>
    ```
    You should be able to find an environment called `CCDI_to_dbGaP_env` when you run 

    ```bash
    conda env list
    ```
- **Activate conda environment**

    All the dependecies that the script requires should be succesfully installed within this environment. To activate the environemnt, simply run

    ```bash
    conda activate CCDI_to_dbGaP_env
    ```

    You should be able to see `(CCDI_to_dbGaP_env)` at the begining of your terminal prompt line after activation.

- **Deactivate conda environment**

    ```bash
    conda deactivate
    ```


### Usage instruction


> **❗Note**: If no **CONSENT NUMBER** is provided, ALL CONSENT IS ASSUMED TO BE GRU, CONSENT GROUP 1.

```
>> python CCDI_to_dbGaP.py --help
usage: CCDI_to_dbGaP.py [-h] -f FILE [-c CONSENT_NUMBER] [-s PREVIOUS_SUBMISSION]

This script is a python version to generate dbGaP submission files using a validated CCDI
submission manifest

required arguments:
  -f FILE, --file FILE  A validated dataset file based on the template
                        CCDI_submission_metadata_template (.xlsx)

optional arguments:
  -c CONSENT_NUMBER, --consent_number CONSENT_NUMBER
                        Number of consent group for the study
  -s PREVIOUS_SUBMISSION, --previous_submission PREVIOUS_SUBMISSION
                        A previous dbGaP submission directory for the same phs_id study.
```

- **Inputs**

    The script requires a validated `CCDI manifest`. The previous SRA submission folder is optional.

- **Outputs**

    - ***A log file*** named in `CCDI_to_dbGaP_<today_date>.log`
    - (If the script finishes successfully) ***A folder*** named in `<phs_id>_dbGaP_submission_<today_date>`. 
        ```
        aviator_falsetto_6_dbGaP_submission_2023-11-24/
        ├── SA_DD.xlsx
        ├── SA_DS_aviator_falsetto_6_dbGaP_submission.txt
        ├── SC_DD.xlsx
        ├── SC_DS_aviator_falsetto_6_dbGaP_submission.txt
        ├── SSM_DD.xlsx
        ├── SSM_DS_aviator_falsetto_6_dbGaP_submission.txt
        └── metadata.json
        
        1 directory, 7 files
        ```
