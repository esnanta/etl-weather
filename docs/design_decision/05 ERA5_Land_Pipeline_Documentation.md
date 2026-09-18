# Dokumentasi Teknis Pipeline ERA5-Land sampai P15

**Proyek:** `etl-weather`  
**Fokus:** ERA5-Land hourly → daily climate → PostgreSQL → controlled modelling access  
**Status dokumentasi:** Snapshot setelah P15 berhasil diverifikasi  
**Tanggal:** 17 September 2026

---

## 1. Tujuan Dokumen

Dokumen ini merangkum arsitektur, kontrak data, desain database, repository layer, proses ETL, validasi, dan keputusan teknis yang telah diselesaikan pada pipeline ERA5-Land.

Dokumen ini dibuat sebagai referensi jangka panjang agar pengembangan berikutnya tidak kehilangan konteks mengenai:

- bagaimana data cuaca diperoleh dan ditransformasikan;
- apa yang dianggap sebagai sumber data kanonik;
- bagaimana PostgreSQL menyimpan hasil daily climate;
- bagaimana data dibaca kembali untuk tahap modelling;
- bagaimana idempotency, checkpoint, dan validasi dijaga;
- keputusan desain penting beserta alasannya;
- apa yang sudah terverifikasi dan apa yang masih menjadi pekerjaan berikutnya.

Pembahasan difokuskan pada arsitektur yang digunakan sekarang.

---

# 2. Gambaran Arsitektur

Pipeline saat ini menggunakan alur berikut:

```text
Copernicus CDS
ERA5-Land hourly
        |
        v
EXTRACT
        |
        v
RAW NetCDF
        |
        v
RAW VALIDATION
        |
        v
TRANSFORM
hourly -> local daily climate
        |
        v
TRANSFORMED Parquet
        |
        v
TRANSFORMED VALIDATION
        |
        v
LOAD
PostgreSQL
        |
        v
climate_data.daily_climate
climate_data.daily_climate_detail
        |
        v
P15 DAILY CLIMATE REPOSITORY
        |
        v
in-memory Pandas DataFrame
        |
        v
P16 / P17 / modelling berikutnya
```

Prinsip utama arsitektur ini adalah memisahkan:

1. **data sumber**;
2. **transformasi meteorologis**;
3. **persistence/database**;
4. **akses data untuk modelling**;
5. **modelling/AI**.

Pemisahan ini penting agar perubahan pada model AI tidak mengubah data iklim kanonik dan agar proses ETL tetap dapat diuji secara independen.

---

# 3. Sumber Data dan Kontrak Meteorologis

Dataset utama yang digunakan adalah:

```text
Source   : CDS
Dataset  : reanalysis-era5-land
Input    : hourly ERA5-Land
Output   : daily climate observations
Timezone : Asia/Jakarta (UTC+7)
```

## 3.1 Variabel

Lima variabel meteorologis yang saat ini dikumpulkan adalah:

| Provider code | Makna | Daily statistic |
|---|---|---|
| `tp` | Total precipitation | total harian |
| `t2m` | 2 metre temperature | mean harian |
| `d2m` | 2 metre dewpoint temperature | mean harian |
| `swvl1` | Volumetric soil water layer 1 | mean harian |
| `ssrd` | Surface solar radiation downwards | total harian |

Provider/native code dipertahankan sebagai vocabulary utama selama proses ETL.

Alasannya:

- sesuai langsung dengan terminologi ERA5-Land;
- mengurangi ambiguity saat dibandingkan dengan dokumentasi Copernicus;
- memudahkan tracing dari API sampai hasil transformasi;
- mencegah terlalu banyak alias yang dapat membingungkan implementasi.

## 3.2 Konversi Unit

Kontrak unit output daily climate adalah:

```text
tp     -> mm/day
t2m    -> degC
d2m    -> degC
swvl1  -> m3/m3
ssrd   -> J/m2/day
```

Kolom database menggunakan nama yang secara eksplisit mencantumkan statistik dan unit:

```text
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

Alasan penggunaan nama eksplisit ini adalah untuk menghindari situasi ketika pengguna database harus menebak apakah suatu nilai adalah hourly, daily, mean, total, Kelvin, Celsius, meter, atau millimeter.

---

# 4. Konsep Waktu dan Local Day

Pipeline menggunakan dua representasi waktu:

```text
valid_time
valid_time_local
```

Keduanya sengaja dipertahankan.

`valid_time` merepresentasikan referensi UTC yang berasal dari data ERA5-Land.

`valid_time_local` merepresentasikan hari lokal yang digunakan oleh aplikasi dan analisis di Aceh/Indonesia.

Timezone operasional:

```text
Asia/Jakarta = UTC+7
```

Local midnight:

```text
00:00 WIB = 17:00 UTC hari sebelumnya
```

Karena itu contoh berikut adalah benar:

```text
valid_time       = 2021-12-31 17:00:00
valid_time_local = 2022-01-01 00:00:00
```

Tidak digunakan kolom generik bernama `date`, karena nama tersebut menghilangkan informasi penting mengenai konteks UTC dan local day.

---

# 5. Agregasi Daily ERA5-Land

Variabel instantaneous:

```text
t2m
d2m
swvl1
```

diubah menjadi mean per local day.

Variabel accumulated:

```text
tp
ssrd
```

menggunakan semantik accumulation ERA5-Land.

Formula local-day yang digunakan:

```text
Daily(D) =
    A(D 17 UTC)
  + A(D 00 UTC)
  - A(D-1 17 UTC)
```

Formula ini penting karena nilai accumulated ERA5-Land tidak boleh diperlakukan seperti penjumlahan hourly biasa.

Kesalahan pada bagian ini dapat menghasilkan nilai precipitation atau solar radiation yang salah walaupun pipeline secara teknis terlihat berjalan.

---

# 6. Storage dan Checkpoint

Pipeline mempertahankan dua artifact utama:

```text
RAW         = NetCDF
TRANSFORMED = Parquet
```

## 6.1 RAW NetCDF

RAW NetCDF dianggap sebagai sumber data ilmiah utama setelah data diperoleh dari Copernicus.

Prinsip:

- RAW dipertahankan;
- RAW tidak ditimpa secara diam-diam;
- request yang sama dapat menggunakan checkpoint yang sudah ada;
- checkpoint divalidasi kembali sebelum digunakan.

Alasan RAW dipertahankan adalah agar transformasi dapat diulang tanpa harus selalu mengunduh ulang data dari CDS.

## 6.2 Transformed Parquet

Setelah transformasi daily selesai, hasil disimpan sebagai Parquet.

Schema transformed utama:

```text
valid_time
valid_time_local
latitude
longitude
tp
t2m
swvl1
ssrd
d2m
```

Parquet berfungsi sebagai checkpoint hasil transformasi sebelum load ke PostgreSQL.

Manfaatnya:

- transform tidak harus selalu diulang;
- debugging lebih mudah;
- ETL dapat dilanjutkan dari tahap tertentu;
- proses lebih sesuai untuk orkestrasi Airflow di masa depan.

---

# 7. Request Identity dan Idempotency

Setiap logical request memiliki deterministic `request_key`.

Identitas request dibangun dari elemen penting seperti:

```text
source
dataset
period
variable set
bounding box
```

Selain itu terdapat `variable_signature`, yaitu representasi deterministic dari variable set.

Tujuannya adalah agar request yang sama dapat dikenali kembali secara konsisten.

Dengan demikian:

```text
same logical request
        ->
same request_key
```

Ini adalah dasar idempotency pada metadata database.

---

# 8. PostgreSQL sebagai Canonical Daily Climate Store

Setelah transformasi dan validasi selesai, daily climate dimasukkan ke PostgreSQL.

Schema database:

```text
climate_data
```

Tabel utama:

```text
climate_data.daily_climate
climate_data.daily_climate_detail
```

Pembagian dua tabel ini memisahkan:

- metadata logical partition;
- observation per grid cell per local day.

---

# 9. Tabel `daily_climate`

`daily_climate` menyimpan metadata satu logical daily-climate partition.

Schema konseptual:

```text
id
request_key
dataset_name
source
start_date
end_date
variable_signature
north_lat
south_lat
east_lon
west_lon
transformed_at
```

## 9.1 Fungsi `request_key`

`request_key` diberi unique index.

Artinya satu logical request hanya memiliki satu identitas metadata.

Pada retry:

```text
INSERT
ON CONFLICT (request_key)
DO UPDATE
```

Alasan menggunakan UPSERT adalah karena ETL harus aman untuk dijalankan kembali.

Retry tidak boleh menghasilkan metadata partition baru untuk request yang sama.

## 9.2 Spatial Metadata

Bounding box disimpan sebagai:

```text
north_lat
south_lat
east_lon
west_lon
```

Bounding box menjadi bagian dari identitas partition karena dua request dengan tanggal dan variabel yang sama tetapi area geografis berbeda bukan dataset yang sama.

---

# 10. Tabel `daily_climate_detail`

Tabel ini menyimpan observation daily per grid cell.

Schema utama:

```text
id
daily_climate_id
valid_time
valid_time_local
latitude
longitude
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

Relasi:

```text
daily_climate_detail.daily_climate_id
        ->
daily_climate.id
```

Foreign key menggunakan:

```text
ON DELETE CASCADE
```

sehingga observation tetap konsisten dengan metadata partition-nya.

---

# 11. Observation Identity

Unique observation key adalah:

```text
daily_climate_id
+ valid_time
+ latitude
+ longitude
```

Database memiliki unique index terhadap kombinasi tersebut.

Pada load digunakan:

```text
ON CONFLICT (...)
DO UPDATE
```

Artinya menjalankan request yang sama berkali-kali tidak menambahkan observation duplicate.

Alasan desain ini adalah untuk memastikan ETL bersifat **idempotent**.

Ini sangat penting sebelum Airflow digunakan karena scheduler biasanya memiliki retry, rerun, dan backfill.

---

# 12. Mapping Provider Variable ke Database

Loader melakukan mapping berikut:

```text
tp
 -> tp_daily_total_mm

t2m
 -> t2m_daily_mean_c

d2m
 -> d2m_daily_mean_c

swvl1
 -> swvl1_daily_mean_m3_m3

ssrd
 -> ssrd_daily_total_j_m2
```

Provider code tetap dipakai selama tahap scientific transformation.

Nama database baru digunakan pada persistence boundary.

Ini memberi pemisahan yang jelas:

```text
scientific/provider vocabulary
        |
        v
database persistence vocabulary
```

---

# 13. Penanganan Missing Value

Missing value tidak pernah dianggap otomatis sebagai nol.

Saat load ke PostgreSQL:

```text
NaN -> NULL
```

Prinsip ini sangat penting terutama untuk precipitation.

Contoh:

```text
tp = 0
```

berarti precipitation benar-benar terobservasi nol.

Sedangkan:

```text
tp = NULL
```

berarti nilai tidak tersedia.

Menyamakan keduanya dapat menghasilkan bias serius pada analisis wet-day dan clustering.

---

# 14. `daily_climate_repository`

File:

```text
pipeline/daily_climate_repository.py
```

Repository ini merupakan boundary antara PostgreSQL dan tahap modelling.

Fungsi utamanya:

```python
load_daily_climate_for_modelling(...)
```

Repository bersifat **read-only**.

Ia tidak:

- membuat table;
- mengubah schema;
- menulis observation;
- melakukan scaling;
- melakukan imputation;
- membuat wet-day label;
- memilih final clustering feature.

Alasan repository dibuat terpisah dari `pipeline/model.py` adalah untuk membedakan dengan jelas:

```text
model.py
    domain/data structures

daily_climate_repository.py
    persistence read/access layer
```

Nama `repository` juga menggambarkan tanggung jawab komponen tersebut dengan lebih jelas dibandingkan istilah generik seperti `modelling_data`.

---

# 15. `DailyClimateSelection`

Repository menggunakan object:

```python
DailyClimateSelection
```

yang menentukan selection secara eksplisit berdasarkan:

```text
source
dataset_name
start_date
end_date
bbox
```

Selection eksplisit diperlukan agar modelling tidak membaca data secara ambigu dari database.

Contoh konsep:

```text
source       = cds
dataset_name = reanalysis-era5-land
period       = 2022-01-01 -> 2022-01-03
bbox         = Aceh test area
```

---

# 16. Query Repository

Query melakukan join:

```text
daily_climate
        |
        +--- daily_climate_detail
```

berdasarkan:

```text
dcd.daily_climate_id = dc.id
```

Filter utama:

```text
source
dataset_name
north_lat
south_lat
east_lon
west_lon
valid_time_local >= start_time
valid_time_local < end_time_exclusive
```

Parameter SQL menggunakan SQLAlchemy `TextClause` dan bind parameter.

Contoh:

```sql
WHERE CAST(dc.source AS TEXT) = :source
```

Query dieksekusi melalui:

```python
connection.execute(statement, params)
```

kemudian hasilnya diubah menjadi Pandas DataFrame.

Alasan eksekusi query dilakukan langsung melalui SQLAlchemy adalah agar parameter binding tetap aman dan konsisten dengan PostgreSQL driver.

---

# 17. Kontrak DataFrame P15

Kolom output repository:

```text
daily_climate_id
valid_time
valid_time_local
latitude
longitude
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

DataFrame disortir secara deterministic berdasarkan:

```text
valid_time_local
latitude
longitude
daily_climate_id
```

Repository tidak membuat physical dataset baru.

Data modelling pada P15 adalah:

```text
controlled database query
        ->
validated in-memory DataFrame
```

Ini menghindari duplikasi data tanpa kebutuhan ilmiah yang jelas.

---

# 18. Validasi P15

Repository melakukan beberapa validasi sebelum data diteruskan ke modelling.

## 18.1 Selection validation

Memastikan:

```text
start_date <= end_date
dataset_name tidak kosong
north_lat >= south_lat
east_lon >= west_lon
```

## 18.2 Required columns

Semua kolom kontrak harus ada.

Jika kolom penting seperti:

```text
valid_time
```

hilang, repository gagal secara eksplisit.

## 18.3 Empty dataset

Selection yang tidak menghasilkan observation dianggap error.

Ini lebih aman daripada meneruskan DataFrame kosong ke modelling dan baru menemukan masalah jauh di downstream.

## 18.4 Duplicate local-grid observation

Uniqueness untuk handoff modelling diperiksa berdasarkan:

```text
valid_time_local
latitude
longitude
```

Jika observation local-day/grid yang sama muncul dari lebih dari satu partition, repository menolak hasil tersebut.

Alasan:

modelling seharusnya menerima satu observation untuk satu:

```text
local day + grid cell
```

bukan dua observation yang hanya berbeda pada metadata partition.

## 18.5 Local midnight

`valid_time_local` harus berada di local midnight.

Ini memastikan daily climate benar-benar mengikuti contract local-day yang telah ditetapkan.

## 18.6 Requested period

Repository memastikan local date hasil query tetap berada dalam:

```text
start_date ... end_date
```

---

# 19. Mengapa PostgreSQL Menjadi Sumber Canonical untuk Modelling

Setelah data selesai melalui:

```text
extract
validation
transform
validation
load
```

PostgreSQL menjadi canonical persisted source untuk akses modelling.

P15 tidak membuat table seperti:

```text
daily_feature_dataset
```

hanya untuk menyalin isi daily climate.

Alasannya:

1. Menghindari redundant persistence.
2. Mengurangi risiko dua physical dataset berbeda isi.
3. Data meteorologis tetap memiliki satu sumber canonical.
4. Feature engineering seharusnya dilakukan pada modelling layer.
5. Wet-day definition belum menjadi fakta meteorologis canonical.
6. Final clustering feature belum diputuskan.

---

# 20. Script Verifikasi P15

Script:

```text
scripts/read_daily_climate.py
```

digunakan sebagai runtime sanity check untuk repository.

Command:

```bash
python scripts/read_daily_climate.py
```

Script melakukan:

```text
build DailyClimateSelection
        |
        v
load_daily_climate_for_modelling()
        |
        v
log repository metrics
        |
        v
ASCII table preview
```

Output menggunakan tabel ASCII agar mudah dibaca pada terminal dan log CI/Airflow.

---

# 21. Bukti Runtime yang Sudah Diverifikasi

P15 telah diuji terhadap PostgreSQL development secara nyata.

Hasil:

```text
source                   = cds
dataset_name             = reanalysis-era5-land
period                   = 2022-01-01 -> 2022-01-03
rows                     = 747
daily_climate_partitions = 1
local_days               = 3
grid_cells               = 249
```

Interpretasi:

```text
249 valid land grid cells
x
3 local days
=
747 observations
```

Nilai tersebut konsisten dengan pipeline transform dan database yang sebelumnya dimuat.

Contoh temporal mapping juga benar:

```text
valid_time       2021-12-31 17:00:00
valid_time_local 2022-01-01 00:00:00
```

---

# 22. Test Suite yang Sudah Terverifikasi

Focused test suite terakhir:

```text
37 passed
```

Test mencakup antara lain:

### Request validation
- provider variable names;
- invalid API variable names;
- unknown variable;
- duplicate variable;
- deterministic variable signature;
- deterministic request key.

### CDS normalization
- normalization time coordinate;
- API names kembali ke provider codes.

### Transform
- UTC dan local time;
- accumulated-variable boundary formula;
- temperature conversion;
- soil moisture unit;
- metadata exclusion.

### Validation
- continuous hourly data;
- missing hour detection;
- spatial grid validation;
- ERA5-Land resolution;
- UTC+7 contract;
- missing local day;
- duplicate grid record;
- grid membership;
- off-grid coordinate.

### Storage
- RAW NetCDF;
- deterministic artifact path;
- immutable RAW checkpoint;
- resume existing RAW checkpoint.

### PostgreSQL load
- canonical table names;
- provider-to-database column mapping;
- metadata request-key UPSERT.

### Daily climate repository
- canonical query;
- invalid date selection;
- missing required column;
- duplicate local-grid detection;
- missing-value preservation;
- deterministic schema dan sorting.

Terdapat `DeprecationWarning` dari dependency `xarray/netCDF4` terhadap NumPy 2.5 pada storage tests. Warning tersebut berasal dari dependency eksternal dan tidak dianggap sebagai kegagalan pipeline.

---

# 23. Idempotency yang Sudah Terbukti

Pipeline pernah dijalankan dua kali untuk logical request yang sama.

Run pertama:

```text
download / transform / load
```

Run berikutnya:

```text
RAW checkpoint resumed
Parquet checkpoint resumed
same request_key
same daily_climate_id
UPSERT observations
```

Jumlah observation tidak bertambah menjadi duplicate.

Ini membuktikan dua aspek penting:

```text
P10 checkpoint/resume
P11 idempotent database load
```

---

# 24. Logging

Pipeline menggunakan structured metric logging berbasis ASCII table.

Contoh:

```text
[METRIC] P15 modelling dataset
+--------------------------+--------------------------+
| field                    | value                    |
+--------------------------+--------------------------+
| source                   | cds                      |
| dataset_name             | reanalysis-era5-land     |
| period                   | 2022-01-01 -> 2022-01-03 |
| rows                     | 747                      |
| daily_climate_partitions | 1                        |
| local_days               | 3                        |
| grid_cells               | 249                      |
+--------------------------+--------------------------+
```

Alasan penggunaan format ini:

- mudah dibaca manusia;
- cocok untuk terminal;
- cocok untuk log Airflow;
- tidak bergantung pada notebook atau GUI;
- metric penting dapat segera terlihat.

---

# 25. Pemisahan Tanggung Jawab Modul

Struktur tanggung jawab utama:

```text
pipeline/model.py
    domain object / request / dataset structure

pipeline/database.py
    PostgreSQL configuration
    database/schema/table creation

pipeline/load.py
    persistence
    metadata UPSERT
    observation UPSERT

pipeline/daily_climate_repository.py
    controlled read-only access
    PostgreSQL -> modelling DataFrame

pipeline/transform.py
    scientific hourly -> daily transformation

pipeline/validation.py
    temporal/spatial/scientific validation

pipeline/storage.py
    RAW and transformed artifact persistence

pipeline/runtime.py
    pipeline stage orchestration
```

Pemisahan ini merupakan keputusan penting karena membuat pipeline lebih mudah:

- diuji;
- dirawat;
- diorkestrasi oleh Airflow;
- digunakan kembali oleh modelling;
- ditelusuri ketika terjadi masalah.

---

# 26. Airflow Boundary

Airflow belum menjadi tempat scientific logic.

Rencana desain:

```text
Airflow
    schedule
    task dependency
    retry
    concurrency
    backfill
    operational alerting
```

Sedangkan pipeline tetap memiliki:

```text
extract
validation
transform
storage
database load
daily climate access
```

Alasannya:

- core pipeline tetap dapat dijalankan tanpa Airflow;
- unit test tidak perlu menginisialisasi Airflow;
- DAG menjadi tipis;
- scientific logic dapat diuji secara langsung.

Untuk future task handoff, XCom sebaiknya hanya membawa metadata kecil atau artifact reference, bukan DataFrame atau xarray Dataset besar.

---

# 27. Status Roadmap

Status sampai dokumentasi ini dibuat:

```text
[x] P4  retain hourly RAW data
[x] P5  UTC+7 local day
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  source metadata excluded from observations
[x] P9  provider variable naming
[x] P10 checkpoint / resume
[x] P11 idempotent PostgreSQL load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P15 controlled daily-climate modelling access
[x] P20 hourly ERA5-Land as primary RAW input

[ ] P18 complete ERA5-Land test coverage
[ ] P19 provenance
[ ] Airflow DAG / historical backfill orchestration
[ ] 10-year historical backfill
[ ] P16 wet-day definition
[ ] P17 feature contribution / feature-set evaluation
[ ] wet-day clustering
```

---

# 28. Mengapa P16 dan P17 Ditunda

P16:

```text
Wet-Day Definition
```

P17:

```text
Feature Contribution / Feature-Set Evaluation
```

belum dilakukan karena keputusan tersebut sebaiknya dibuat setelah data historis yang cukup tersedia.

Rencana dataset adalah sekitar 10 tahun.

Alasan:

- threshold wet-day sebaiknya dievaluasi terhadap distribusi data nyata;
- kontribusi feature tidak seharusnya diputuskan dari sampel tiga hari;
- lima parameter meteorologis saat ini adalah candidate parameters;
- belum ada keputusan bahwa semuanya harus menjadi input clustering.

Dengan demikian urutan yang dipilih adalah:

```text
P15
  |
  v
P18
  |
  v
P19
  |
  v
Airflow / backfill
  |
  v
10-year data
  |
  v
P16
  |
  v
P17
  |
  v
clustering
```

---

# 29. Hal yang Perlu Dijaga pada Pengembangan Berikutnya

Beberapa prinsip yang sebaiknya dianggap sebagai contract:

## 29.1 Jangan mengubah missing menjadi zero secara global

```text
NULL != 0
```

## 29.2 Jangan menghapus `valid_time`

UTC reference tetap dibutuhkan untuk provenance dan scientific traceability.

## 29.3 Jangan mengganti `valid_time_local` menjadi `date`

Nama eksplisit lebih aman.

## 29.4 Jangan memindahkan scientific logic ke DAG

Airflow hanya orchestrator.

## 29.5 Jangan membuat persistent modelling dataset tanpa kebutuhan jelas

Gunakan canonical PostgreSQL daily climate sebagai source.

## 29.6 Jangan menentukan final clustering feature di ETL

Feature selection adalah modelling concern.

## 29.7 Retry harus tetap idempotent

Backfill dan Airflow akan sangat bergantung pada hal ini.

---

# 30. Catatan Teknis yang Masih Perlu Diperhatikan

Walaupun pipeline saat ini sudah stabil untuk sanity dataset, beberapa area masih layak diperkuat sebelum historical backfill besar.

## 30.1 Artifact path dan spatial partition

Request identity sudah memasukkan bounding box.

Namun deterministic artifact filename perlu tetap dievaluasi agar dua request period/variable yang sama tetapi bbox berbeda tidak berpotensi menunjuk nama file yang sama.

Ini penting sebelum pipeline digunakan untuk banyak spatial partition.

## 30.2 Provenance

Checkpoint saat ini terutama mengikuti input/request identity.

P19 perlu menambahkan provenance yang dapat menjawab:

```text
data ini dibuat oleh versi transform yang mana?
commit/code version apa?
configuration apa?
kapan dibuat?
```

Ini penting karena hasil transformasi dapat berubah jika scientific code berubah walaupun request input tetap sama.

## 30.3 Repository selection dan variable signature

P15 saat ini memilih partition berdasarkan:

```text
source
dataset
period
bbox
```

Database juga memiliki:

```text
variable_signature
```

Ketika di masa depan terdapat heterogeneous partition dengan variable set berbeda pada area dan periode yang overlap, repository selection perlu dievaluasi apakah `variable_signature` harus menjadi bagian eksplisit dari selection contract.

Saat ini tidak menjadi masalah karena pipeline aktif menggunakan lima variabel ERA5-Land yang sama.

## 30.4 Large historical reads

P15 saat ini mengembalikan Pandas DataFrame in-memory.

Ini sesuai untuk tahap sekarang.

Namun setelah historical dataset mencapai banyak tahun, perlu diukur:

```text
row count
RAM usage
query latency
modelling access pattern
```

Jika dibutuhkan, repository dapat dikembangkan dengan:

- chunked reads;
- period partitioning;
- spatial subset;
- server-side aggregation;
- Arrow/Polars/DuckDB interoperability.

Optimasi tersebut sebaiknya dilakukan berdasarkan ukuran data nyata, bukan asumsi sebelum backfill.

---

# 31. Development Verification Commands

## Focused tests

```bash
pytest \
  tests/unit/test_utils_request_validation.py \
  tests/unit/test_extract_cds_normalization.py \
  tests/unit/test_transform.py \
  tests/unit/test_validation.py \
  tests/unit/test_storage.py \
  tests/unit/test_load.py \
  tests/unit/test_daily_climate_repository.py -v
```

Expected verified state:

```text
37 passed
```

## Repository runtime read

```bash
python scripts/read_daily_climate.py
```

Current sanity expectation:

```text
rows                     747
daily_climate_partitions 1
local_days               3
grid_cells               249
```

---

# 32. Ringkasan Keputusan Arsitektur

Keputusan terpenting yang telah diambil adalah:

```text
ERA5-Land hourly
    = scientific source

RAW NetCDF
    = retained source checkpoint

Daily Parquet
    = transformed checkpoint

PostgreSQL daily_climate
    = canonical persisted daily climate

daily_climate_repository
    = controlled read-only modelling access

Pandas DataFrame
    = current in-memory modelling handoff
```

Arsitektur ini dipilih agar:

1. data ilmiah dapat dilacak kembali ke RAW;
2. transformasi tidak perlu dilakukan ulang pada setiap tahap;
3. retry aman;
4. database tidak menghasilkan duplicate observation;
5. modelling tidak mengubah climate truth;
6. Airflow dapat ditambahkan tanpa memindahkan scientific logic;
7. backfill 10 tahun dapat dilakukan dengan fondasi yang lebih aman;
8. P16/P17 dapat dievaluasi berdasarkan dataset representatif, bukan berdasarkan asumsi.

---

# 33. Kesimpulan

Fondasi ETL ERA5-Land saat ini sudah memiliki jalur lengkap:

```text
CDS
 ->
hourly RAW
 ->
validated daily transformation
 ->
checkpoint
 ->
idempotent PostgreSQL load
 ->
controlled repository access
 ->
validated modelling DataFrame
```

P15 telah terverifikasi melalui unit test dan real PostgreSQL read.

Dengan kondisi ini, pipeline sudah memiliki pemisahan yang jelas antara:

```text
scientific data processing
database persistence
data access
future modelling
```

Tahap selanjutnya sebaiknya berfokus pada penguatan test coverage dan provenance sebelum pipeline digunakan untuk historical backfill skala besar.

---

## Referensi File Utama

```text
pipeline/model.py
pipeline/database.py
pipeline/load.py
pipeline/daily_climate_repository.py
pipeline/transform.py
pipeline/validation.py
pipeline/storage.py
pipeline/runtime.py

scripts/run_pipeline_cds.py
scripts/read_daily_climate.py

tests/unit/test_utils_request_validation.py
tests/unit/test_extract_cds_normalization.py
tests/unit/test_transform.py
tests/unit/test_validation.py
tests/unit/test_storage.py
tests/unit/test_load.py
tests/unit/test_daily_climate_repository.py

Readme-pipeline.md
tests/README_TESTING.md
```
