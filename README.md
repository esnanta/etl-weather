# Weather Dataset Project

This project contains weather data for the Aceh region from 2019-2024 in NetCDF (.nc) format, downloaded from the Copernicus Climate Data Store (CDS).

## Project Structure

```
weather-dataset/
├── myvenv/                 # Python virtual environment
├── data_iklim/            # NetCDF data for Aceh (2019-2024)
├── requirements.txt       # List of Python dependencies
├── aceh_climate.db        # SQLite database
└── *.ipynb               # Jupyter notebooks
```

## Getting Started

Follow these steps to set up your local development environment.

### 1. Set Up the Virtual Environment

A Python virtual environment is used to manage project-specific dependencies. The `myvenv` directory is included in this project.

To activate it, run the following command from the project root:

```bash
python3 -m venv myvenv
source myvenv/bin/activate
```

Once activated, your terminal prompt will be prefixed with `(myvenv)`.

> **Note**: If you don't have the `myvenv` directory, you can create it using `python -m venv myvenv`.

### 2. Install Dependencies

After activating the virtual environment, install the required Python packages using the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

This command installs all the necessary libraries, such as `pandas`, `xarray`, and `jupyter`.

## Usage

### Running Jupyter Notebook

To start an interactive analysis session, run Jupyter Notebook or JupyterLab.

For Jupyter Notebook:
```bash
jupyter notebook
```

For JupyterLab:
```bash
jupyter lab
```

### Code Examples

Here are some basic examples of how to work with the data.

#### Reading NetCDF Files

```python
import xarray as xr

# Open a NetCDF file
ds = xr.open_dataset('data_iklim/aceh_data_2024_01.nc')

# Display dataset information
print(ds)

# Access a specific variable (e.g., 2m temperature)
temperature = ds['t2m']
print(temperature)
```

#### Analysis with Pandas

```python
import pandas as pd
import xarray as xr

# Convert an xarray Dataset to a pandas DataFrame
ds = xr.open_dataset('data_iklim/aceh_data_2024_01.nc')
df = ds.to_dataframe()
print(df.head())
```

## Managing Dependencies

It's important to keep the `requirements.txt` file up-to-date.

### Adding a New Package

If your analysis requires a new package, install it using pip:

```bash
pip install new-package-name
```

After installation, update the `requirements.txt` file:

```bash
pip freeze > requirements.txt
```

### Checking for Outdated Packages

To see if any installed packages have newer versions available, run:

```bash
pip list --outdated
```

You can then upgrade packages individually.

## Troubleshooting

### Error: `ModuleNotFoundError`

This usually means your virtual environment is not active. Activate it with:
```bash
source myvenv/bin/activate
```

### Problems with `pip`

If you encounter issues during package installation, try upgrading `pip` itself:
```bash
pip install --upgrade pip
```

## Notes

- This virtual environment uses Python 3.12.3.
- The weather data covers the period 2019-2024 for the Aceh region.
- Data format: NetCDF (.nc).
- Data source: Copernicus Climate Data Store (ERA5).
