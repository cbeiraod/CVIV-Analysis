# CVIV-Analysis

Script for analysing and processing the CVIV from LGAD measurements.

 * `compare.py` - This scripts compares the contents of an in directory and a compare directory in order to make sure all the files in the in directory also exist and have the same contents as the cmp directory. Please pay attention to the output of this script when using it.
 * `load_runs.py` - This script loads the runs into the database and keeps a copy of the data in the database as well. This shold be the first 'analysis' script to run.
 * `print_run_summary.py` - This script prints a summary of all the runs.
 * `set_run_observation.py` - This script allows to set the observation field for specific runs.
 * `load_df.py` - This script loads the data from the original file and loads it into a dataframe, subsequently saving the dataframe in CSV format into the run directory.
 * `plot_iv.py` - This script plots the IV curves both for CV runs and IV runs
 * `plot_cv.py` - This script plots the CV curves for CV runs

## Dependencies

Some of the scripts in the repository use the 'LIP-PPS-Run-Manager', 'plotly', 'pandas' and 'pyarrow' libraries, please install them in order to use the scripts. I suggest using a venv for keeping environments separate and installing what is needed for specific use cases.

### Venv installation

Run the command: `python -m venv venv`, this will create a venv in the venv directory

Remember to always activate the venv before using it: `source venv/bin/activate`

And deactivate the venv once you are done: `deactivate`

### Library installation

Install the libraries, after activating the venv if using one, with:
 * `python -m pip install --upgrade pip`
 * `python -m pip install lip-pps-run-manager`
 * `python -m pip install plotly`
 * `python -m pip install pandas`
 * `python -m pip install pyarrow`