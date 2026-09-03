# PYTHON CODING GUIDELINES FOR AIRFLOW ETL

This document focuses on Python ETL implementation, scientific
correctness, data contracts, validation, testing, and coding style.
Airflow DAG design, task orchestration, retries, sensors, concurrency,
and deployment concerns live in `AIRFLOW_BEST_PRACTICE.md`.

## Scientific Correctness Is Part of Software Correctness

In weather-data pipelines, technically valid code can still produce scientifically incorrect data.

Examples include:

- treating meters as millimeters;
- treating Kelvin as Celsius;
- confusing UTC with local time;
- replacing missing values with zero without domain justification;
- mixing provider-specific variables with canonical variables;
- applying formulas without documenting assumptions.

Therefore:

> **Scientific correctness is part of software correctness.**

The coding practices in this document are built around four foundations:

```text
            WEATHER ETL CODE
                  │
       ┌──────────┼──────────┐
       ▼          ▼          ▼
   Simplicity   Software   Scientific
     KISS       Design     Correctness
                  │
                SOLID
                  │
                  ▼
             Testability
```

---

## 1. Follow KISS — Keep It Simple and Straightforward

Prefer the simplest solution that is correct, readable, testable, and maintainable.

Core ideas:

- **Simplicity** — choose direct solutions over unnecessarily complex designs.
- **No Over-Engineering** — do not introduce abstractions, patterns, services, or layers that solve no current problem.
- **Clarity** — code should be easy to read, review, test, and debug.
- **Explicitness** — prefer obvious data flow and dependencies over hidden behavior.

Good:

```python
def calculate_daily_average(values: list[float]) -> float:
    return sum(values) / len(values)
```

Avoid unnecessary abstraction:

```python
class DailyAverageCalculationStrategyFactory:
    ...
```

when only one calculation method exists.

Principle:

> Prefer the simplest design that preserves correctness and future maintainability.

---

## 2. Apply SOLID Pragmatically

SOLID principles should guide design decisions, not force unnecessary abstraction.

Do not create interfaces, base classes, factories, or dependency layers simply to claim SOLID compliance.

### 2.1 Single Responsibility Principle

A module, class, or function should have one primary responsibility.

For a weather ETL pipeline:

```text
extract.py
    ↓
retrieve weather data

validation.py
    ↓
validate raw records

transform.py
    ↓
normalize and derive weather variables

load.py
    ↓
persist processed data

storage.py
    ↓
manage artifacts and storage
```

Avoid:

```python
def process_weather():
    download_data()
    validate_data()
    clean_data()
    calculate_statistics()
    save_database()
    upload_file()
    send_notification()
```

Prefer separate responsibilities.

---

## 3. Keep ETL Modules Executable and Testable

Execution wrappers should stay thin. The orchestration rules and DAG
structure belong in `AIRFLOW_BEST_PRACTICE.md`.

ETL modules should contain data-processing logic that is executable and
testable without Airflow.

Business logic:

```python
def transform_weather_data(data: pd.DataFrame) -> pd.DataFrame:
    ...
```

The transformation function should not require Airflow to run.

Principle:

> Pipeline code must remain usable and testable independently from Airflow.

---

## 4. Prefer Functions for Stateless ETL Logic

Most ETL transformations are naturally expressed as functions.

Prefer:

```python
def convert_kelvin_to_celsius(series: pd.Series) -> pd.Series:
    return series - 273.15
```

instead of:

```python
class TemperatureConverter:
    def convert(self, series):
        ...
```

unless state or multiple interchangeable behaviors genuinely justify a class.

Use classes when they provide clear value, such as:

- configuration objects;
- API clients;
- database repositories;
- storage adapters;
- domain objects with meaningful state.

Do not convert every function into a class.

---

## 5. Keep Functions Small and Focused

A function should perform one coherent operation.

Good:

```python
def normalize_temperature(df):
    ...

def normalize_precipitation(df):
    ...

def calculate_wind_speed(df):
    ...
```

Then compose them:

```python
def transform_weather_data(df):
    df = normalize_temperature(df)
    df = normalize_precipitation(df)
    df = calculate_wind_speed(df)
    return df
```

Avoid functions hundreds of lines long containing extraction, cleaning, transformation, validation, and loading.

---

## 6. Make Data Transformation Explicit

Weather data transformations should be easy to trace.

Prefer:

```python
df["temperature_c"] = df["temperature_k"] - 273.15
```

over hidden or overly generic transformation engines when simple code is sufficient.

For derived variables, use named functions:

```python
def calculate_wind_speed(u10: pd.Series, v10: pd.Series) -> pd.Series:
    return np.sqrt(u10**2 + v10**2)
```

This makes the scientific meaning visible in the code.

---

## 7. Preserve Raw Data

Raw weather data should be treated as immutable input.

Never modify the original dataset in place if the raw layer is intended for reproducibility.

Prefer:

```text
RAW
 ↓
VALIDATED
 ↓
PROCESSED
 ↓
EXPORT / DATABASE
```

For example:

```text
data/
├── raw/
├── processed/
└── export/
```

Principle:

> Raw data represents source truth and should remain reproducible.

---

## 8. Make Units Explicit

Weather data is highly sensitive to unit mistakes.

Do not rely on variable names such as:

```python
temperature
rain
wind
```

when the unit is ambiguous.

Prefer:

```python
temperature_k
temperature_c
precipitation_m
precipitation_mm
wind_speed_ms
pressure_pa
pressure_hpa
```

Conversion functions should clearly communicate units:

```python
def kelvin_to_celsius(value):
    return value - 273.15
```

Avoid implicit unit conversion hidden inside unrelated functions.

---

## 9. Never Assume Missing Values Are Zero

Missing weather observations have different meaning from zero measurements.

For example:

```text
rainfall = 0 mm
```

means no rainfall was recorded.

Whereas:

```text
rainfall = NaN
```

means no valid measurement is available.

Do not write:

```python
df.fillna(0)
```

globally.

Handle missing values according to the meaning of each variable.

---

## 10. Validate Schema Before Transformation

Validate incoming datasets before applying transformations.

Check:

- required columns;
- expected data types;
- timestamp validity;
- missing values;
- duplicate records;
- expected units;
- geographical coordinates;
- reasonable value ranges.

Example:

```python
REQUIRED_COLUMNS = {
    "timestamp",
    "latitude",
    "longitude",
    "temperature_k",
    "precipitation_m",
}
```

Then:

```python
missing = REQUIRED_COLUMNS - set(df.columns)

if missing:
    raise ValueError(f"Missing required columns: {sorted(missing)}")
```

Do not allow malformed data to silently continue downstream.

---

## 11. Define Data Contracts

Each ETL stage should have a clearly defined input and output contract.

Example:

### Extract Output

```text
timestamp
latitude
longitude
t2m
tp
u10
v10
```

### Transform Output

```text
timestamp
latitude
longitude
temperature_c
precipitation_mm
wind_speed_ms
```

Code and tests should enforce these contracts.

This prevents upstream changes from silently breaking downstream processing.
Task handoff conventions and artifact passing rules are documented in
`AIRFLOW_BEST_PRACTICE.md`.

---

## 12. Treat Time Explicitly

Weather ETL is fundamentally time-series processing.

Always distinguish:

- observation time;
- forecast initialization time;
- forecast valid time;
- ingestion time;
- processing time.

Prefer timezone-aware timestamps.

Example:

```python
pd.to_datetime(df["timestamp"], utc=True)
```

Avoid naive datetime values when datasets may span multiple systems or regions.

Never use `datetime.now()` to determine the logical period being processed when Airflow already provides a data interval.

For partitioning and Airflow interval semantics, see
`AIRFLOW_BEST_PRACTICE.md`.

---

## 13. Keep Transformations Deterministic

Given:

```text
same input
+
same configuration
```

the transformation should produce the same result.

Avoid hidden dependencies on:

- current time;
- global state;
- random values;
- machine-specific paths;
- execution order.

If randomness is genuinely required, use an explicit seed.

---

## 14. Prefer Pure Functions Where Practical

A pure transformation function receives data and returns transformed data without external side effects.

Good:

```python
def convert_precipitation_to_mm(df):
    result = df.copy()
    result["precipitation_mm"] = result["tp"] * 1000
    return result
```

This is easier to test than a function that simultaneously:

- transforms data;
- writes a file;
- inserts records into PostgreSQL;
- updates an Airflow Variable.

Separate computation from side effects.

---

## 15. Avoid Unnecessary Mutation

Avoid modifying input objects unpredictably.

Prefer:

```python
def transform(df):
    result = df.copy()
    ...
    return result
```

when mutation could surprise callers.

For large datasets where copying has significant memory cost, intentional in-place operations may be acceptable, but they should be explicit and documented.

---

## 16. Fail Fast on Invalid Scientific Assumptions

Do not silently accept physically impossible or clearly invalid values.

Examples may include:

```text
relative humidity < 0%
relative humidity > 100%
invalid latitude
invalid longitude
negative wind speed
impossible timestamps
```

Validation rules should reflect the source dataset and physical variable semantics.

Do not blindly apply generic thresholds without knowing the dataset specification.

---

## 17. Separate Source-Specific Logic from Canonical Data Models

Weather data may come from:

```text
ERA5
ERA5-Land
BMKG
OpenWeather
other APIs
```

Each source may use different:

- field names;
- units;
- timestamps;
- coordinate conventions;
- missing-value conventions.

Source adapters should convert provider-specific structures into a canonical internal schema.

Conceptually:

```text
ERA5
   \
BMKG ----> Canonical Weather Schema ----> Transformation
   /
API
```

Do not allow provider-specific field names to spread throughout the entire application.

---

## 18. Avoid Magic Numbers

Bad:

```python
temperature = value - 273.15
rainfall = value * 1000
```

when the meaning is not obvious in context.

Prefer named constants where they improve clarity:

```python
KELVIN_TO_CELSIUS_OFFSET = 273.15
METERS_TO_MILLIMETERS = 1000
```

However, do not create constants for obvious values simply to create abstraction.

---

## 19. Use Type Hints

Public functions should normally include type annotations.

Example:

```python
def transform_weather_data(
    data: pd.DataFrame,
) -> pd.DataFrame:
    ...
```

For path handling:

```python
from pathlib import Path

def load_weather_file(path: Path) -> pd.DataFrame:
    ...
```

Type hints improve readability, IDE support, refactoring, and static analysis.

---

## 20. Prefer `pathlib` for File Paths

Prefer:

```python
from pathlib import Path

raw_path = Path("data") / "raw" / "weather.parquet"
```

instead of manually concatenating strings:

```python
raw_path = "data/" + "raw/" + "weather.parquet"
```

This improves portability and readability.

---

## 21. Use Meaningful Names

Use domain terminology.

Prefer:

```python
precipitation_mm
temperature_c
relative_humidity
wind_speed_ms
observation_time
latitude
longitude
```

Avoid:

```python
x
temp2
data_new
result_final
weather2
df_final_final
```

Names should explain both the domain concept and, where relevant, the unit.

---

## 22. Follow Standard Python Style

Follow PEP 8 unless the project defines a justified exception.

Recommended conventions:

```text
functions      → snake_case
variables      → snake_case
modules        → snake_case
classes        → PascalCase
constants      → UPPER_SNAKE_CASE
```

Use automated tooling where appropriate, such as:

```text
Ruff
Black
mypy or Pyright
pytest
```

The exact toolchain should remain consistent across the project.

---

## 23. Keep Imports Explicit and Organized

Avoid:

```python
from module import *
```

Prefer:

```python
from pipeline.transform import transform_weather_data
```

Keep imports grouped logically:

```python
# Standard library

# Third-party libraries

# Local application modules
```

Avoid expensive or side-effecting imports in DAG files.

---

## 24. Handle Exceptions at the Correct Layer

Do not catch every exception simply to continue execution.

Bad:

```python
try:
    ...
except Exception:
    pass
```

Also avoid:

```python
except Exception:
    return None
```

when failure should stop the pipeline.

Catch exceptions when you can:

- add useful context;
- recover safely;
- translate infrastructure errors;
- perform required cleanup.

Otherwise, allow the exception to propagate so Airflow can correctly mark the task as failed.

---

## 25. Preserve Error Context

When wrapping exceptions:

```python
try:
    fetch_weather_data()
except RequestException as exc:
    raise WeatherExtractionError(
        "Failed to retrieve ERA5 weather data"
    ) from exc
```

Using `from exc` preserves the original exception chain.

Errors should explain:

```text
what operation failed
which source was involved
which partition was processed
```

without leaking credentials.

---

## 26. Use Logging, Not `print()`

Application and ETL modules should use Python logging.

Prefer:

```python
import logging

logger = logging.getLogger(__name__)

logger.info(
    "Processed weather records",
    extra={"record_count": len(df)},
)
```

rather than:

```python
print("done")
```

Airflow can capture Python logger output while preserving log levels and context.

---

## 27. Never Log Secrets or Entire Datasets

Do not log:

- API keys;
- passwords;
- access tokens;
- credentials;
- complete API responses;
- full DataFrames.

Log metadata instead:

```text
provider=era5
records=8760
year=2025
variable=t2m
```

This provides observability without unnecessary exposure or excessive logs.

---

## 28. Keep External I/O Behind Clear Boundaries

Network, database, and filesystem access should remain easy to identify.

Examples:

```text
ERA5Client
WeatherRepository
ArtifactStorage
```

or focused functions:

```python
fetch_era5_data(...)
save_raw_dataset(...)
load_processed_dataset(...)
```

Transformation functions should generally not perform hidden external I/O.

---

## 29. Apply Dependency Inversion Where It Provides Value

Core transformation logic should not depend unnecessarily on specific infrastructure.

For example:

```text
Transformation
      │
      └── should not care whether the raw data came from
          ERA5, local files, PostgreSQL, or an API
```

But avoid creating interfaces purely for theoretical compliance.

Introduce abstractions when there are genuine interchangeable implementations or when infrastructure coupling harms testing and maintainability.

---

## 30. Prefer Composition Over Inheritance

Python ETL code rarely needs deep inheritance hierarchies.

Prefer composing small components:

```text
WeatherExtractor
      +
WeatherValidator
      +
WeatherTransformer
      +
WeatherStorage
```

over:

```text
BasePipeline
  ↓
BaseWeatherPipeline
  ↓
HistoricalWeatherPipeline
  ↓
HistoricalERA5WeatherPipeline
```

unless inheritance genuinely represents stable substitutable behavior.

---

## 31. Keep Interfaces Small

When using Python protocols or abstract interfaces, expose only the behavior required by consumers.

Prefer:

```python
class WeatherSource(Protocol):
    def fetch(self, start, end):
        ...
```

rather than a large interface containing unrelated responsibilities.

This follows the Interface Segregation Principle while keeping abstractions practical.

---

## 32. Test Transformations with Small Representative Data

Unit tests should use small datasets covering relevant cases.

Examples:

```text
normal record
missing value
boundary value
invalid unit
duplicate timestamp
zero precipitation
extreme but valid weather value
```

Do not depend on downloading 30 years of weather data for a unit test.

Unit tests should be:

```text
fast
deterministic
isolated
repeatable
```

---

## 33. Separate Unit Tests from Integration Tests

A useful structure is:

```text
tests/
├── unit/
│   ├── test_validation.py
│   ├── test_transform.py
│   └── test_storage.py
│
└── integration/
    └── test_weather_pipeline.py
```

Unit tests should not require:

- Airflow;
- external APIs;
- production databases;
- internet access.

Integration tests may verify multiple real components together.

---

## 34. Mock External Systems, Not Core Business Logic

In unit tests, mock:

```text
HTTP API
database
object storage
Google Sheets
external services
```

Do not mock the transformation logic that the test is supposed to verify.

For example:

```text
mock_api()
real_transform()
temporary_storage()
```

is usually more useful than mocking the entire pipeline.

---

## 35. Keep Configuration Typed and Centralized

Avoid spreading environment variable reads throughout the project.

Bad:

```python
os.getenv("API_KEY")
```

inside many unrelated modules.

Prefer centralized configuration:

```python
@dataclass(frozen=True)
class WeatherPipelineConfig:
    raw_dir: Path
    processed_dir: Path
    provider: str
```

Load environment-specific configuration at the application boundary and pass it explicitly.

---

## 36. Avoid Global Mutable State

Do not use shared mutable module-level objects for runtime pipeline state.

Avoid:

```python
DATA = []
CURRENT_FILE = None
```

Task execution should derive its state from explicit inputs, configuration, or persisted artifacts.

This is particularly important because Airflow tasks may execute in different processes or workers.

---

## 37. Optimize Only After Measuring

Do not sacrifice readability for speculative performance improvements.

Start with:

```text
correct
simple
testable
observable
```

Then measure:

```text
CPU
memory
I/O
network
execution duration
```

Optimize actual bottlenecks.

For large weather datasets, move beyond Pandas when measurements demonstrate that Pandas is the limiting factor.

---

## 38. Prefer Vectorized Operations for Tabular Data

When using Pandas or NumPy, prefer vectorized operations over row-by-row loops where practical.

Prefer:

```python
df["temperature_c"] = df["temperature_k"] - 273.15
```

over:

```python
for index, row in df.iterrows():
    ...
```

This is usually clearer and significantly faster.

But readability and correctness remain more important than clever one-line expressions.

---

## 39. Keep Scientific Transformations Documented

Derived weather variables should document:

- formula;
- units;
- assumptions;
- expected inputs;
- source/reference when scientifically relevant.

Example:

```python
def calculate_wind_speed(
    u_component_ms: pd.Series,
    v_component_ms: pd.Series,
) -> pd.Series:
    """Calculate horizontal wind speed in m/s from u and v components."""
    return np.sqrt(
        u_component_ms.pow(2) + v_component_ms.pow(2)
    )
```

This is especially important when transformations represent scientific or meteorological meaning rather than generic software operations.

---

# Core Coding Principles

For this project, the main coding principles can be summarized as:

1. **Keep solutions simple and explicit.**
2. **Apply SOLID pragmatically, not mechanically.**
3. **Separate orchestration from data-processing logic.**
4. **Keep functions and modules focused on clear responsibilities.**
5. **Prefer stateless and deterministic transformations.**
6. **Preserve raw weather data and explicit data lineage.**
7. **Make units, timestamps, schemas, and data contracts explicit.**
8. **Validate data before transforming it.**
9. **Treat missing data according to domain meaning.**
10. **Keep provider-specific logic outside the canonical transformation layer.**
11. **Separate computation from external side effects.**
12. **Use explicit configuration instead of hidden global state.**
13. **Write code that can be tested without Airflow.**
14. **Handle failures explicitly and preserve useful error context.**
15. **Optimize only after measuring actual bottlenecks.**
