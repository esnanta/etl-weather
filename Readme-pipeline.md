# Weather Dataset Pipeline

## Overview

This document describes a simple ETL pipeline to extract, validate, transform, and store weather data from the ARCO ERA5 dataset using Python, xarray, and other open-source libraries.

The pipeline is designed to be:
* **Simple**: Easy to understand and maintain.
* **Testable**: Each component can be tested independently.
* **Orchestration-Ready**: Can be easily integrated into an orchestrator like Apache Airflow.

## Pipeline Architecture

The pipeline is divided into several layers, each with a specific responsibility.

```
[ WeatherRequest ]
       |
       v
+----------------+
|    EXTRACT     |
+----------------+
       |
       v
[ RawWeatherDataset ]
       |
       v
+----------------+
| VALIDATE (RAW) |
+----------------+
       |
       v
+----------------+
|  SAVE (RAW)    |
+----------------+
       |
       v
+----------------+
|  LOAD (RAW)    |
+----------------+
       |
       v
+----------------+
|   TRANSFORM    |
+----------------+
       |
       v
[ TransformedWeatherDataset ]
       |
       v
+----------------------+
| VALIDATE (TRANSFORMED) |
+----------------------+
       |
       v
+----------------------+
| EXPORT (TRANSFORMED) |
+----------------------+
       |
       v
[ CSV / PostgreSQL ]
```

## Core Components

### `pipeline/extract.py`
- Defines the `WeatherRequest` and `RawWeatherDataset` data structures.
- Connects to the ARCO ERA5 dataset on Google Cloud Storage.
- Downloads data based on the specified variables, date range, and bounding box.
- Normalizes the downloaded data and metadata.

### `pipeline/transform.py`
- Defines the `TransformedWeatherDataset` data structure.
- Currently, this is a placeholder for future data transformations. The data is passed through without modification.

### `pipeline/validation.py`
- Validates the structure, coordinates, and data integrity of both raw and transformed datasets.
- Checks for missing values and ensures data falls within expected physical ranges.

### `pipeline/storage.py`
- Manages the storage of datasets as NetCDF files.
- Organizes data into `raw` and `transformed` layers.
- Provides functions to save, load, and export datasets to CSV.

### `pipeline/database.py`
- Contains functions to connect to a PostgreSQL database.
- Handles schema creation and data insertion from an xarray Dataset.

### `scripts/run_pipeline.py`
- An executable script that runs the end-to-end pipeline.
- It builds a `WeatherRequest`, extracts the data, validates it, saves the raw version, transforms it, validates the transformed version, and finally exports it to a CSV file.

## Data Storage

- **Raw Data**: Stored in NetCDF format under `data/raw/`.
- **Transformed Data**: Stored in NetCDF format under `data/transformed/`.
- **Exports**: Transformed data is exported to CSV files.

The directory structure for stored artifacts is:
```
data/
├── raw/
│   └── <dataset-name>/
│       └── <year>/
│           └── <month>/
│               └── <day>/
│                   └── <dataset-name>_<timestamp>.nc
└── transformed/
    └── <dataset-name>/
        └── <year>/
            └── <month>/
                └── <day>/
                    └── <dataset-name>_<timestamp>.csv
```

## How to Run the Pipeline

The pipeline can be executed by running the `run_pipeline.py` script:

```bash
python scripts/run_pipeline.py
```

This will execute the predefined request in the script and save the output to the `data/transformed` directory. You can modify `scripts/run_pipeline.py` to change the request parameters (dates, variables, bounding box).

## Supported Datasets and Variables

### Dataset
- `era5-single-levels`

### Variables
- `tp` (total precipitation)
- `ssrd` (surface solar radiation downwards)
- `t2m` (2m temperature)
- `u10` (10m u-component of wind)
- `v10` (10m v-component of wind)
- `swh` (significant height of combined wind waves and swell)
- `msl` (mean sea level pressure)

## External Resources
- [ARCO-ERA5 GitHub Repository](https://github.com/google-research/arco-era5)
- [ARCO-ERA5 Data Description](https://github.com/google-research/arco-era5/blob/main/README.md#analysis-ready-data)
- [Colab Notebook Examples](https://github.com/google-research/arco-era5/blob/main/README.md#get-started-with-colab-notebooks)
