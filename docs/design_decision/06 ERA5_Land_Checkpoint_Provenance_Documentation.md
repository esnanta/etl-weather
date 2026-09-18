# Dokumentasi Teknis Pipeline ERA5-Land — P18, P19, dan Checkpoint

**Proyek:** `etl-weather`  
**Branch pengembangan:** `dev`  
**Tahap:** Setelah P18 Test Coverage Hardening dan P19 Provenance  
**Posisi roadmap:** Siap menuju desain Airflow DAG ETL  
**Tanggal:** 18 September 2026

---

## 1. Tujuan Dokumen

Dokumen ini melanjutkan dokumentasi pipeline ERA5-Land yang telah dibangun sampai P15.

Fokus utama dokumen ini adalah:

- P18 — Test Coverage Hardening;
- P19 — Provenance;
- mekanisme checkpoint;
- arti status `created` dan `resumed`;
- validasi setelah resume;
- checksum RAW dan transformed artifact;
- idempotent PostgreSQL load;
- hubungan antara RAW NetCDF, transformed Parquet, dan database;
- kondisi pipeline sebelum masuk ke tahap Airflow DAG ETL.

Dokumen ini dibuat agar pengembangan berikutnya memiliki referensi teknis yang jelas mengenai apa yang sudah selesai, alasan desainnya, dan kontrak yang tidak boleh berubah secara implisit.

---

# 2. Posisi Pipeline Saat Ini

Alur pipeline saat ini:

```text
CDS ERA5-Land hourly
        |
        v
EXTRACT OR RESUME
        |
        v
RAW NetCDF checkpoint
        |
        v
RAW validation
        |
        v
NetCDF -> Pandas DataFrame
        |
        v
TRANSFORM OR RESUME
        |
        v
Daily transformed Parquet checkpoint
        |
        v
Transformed validation
        |
        v
IDEMPOTENT POSTGRESQL LOAD
        |
        v
climate_data.daily_climate
climate_data.daily_climate_detail
        |
        v
P15 Controlled Daily Climate Repository
        |
        v
in-memory Pandas DataFrame
        |
        v
future modelling stages
```

Status:

```text
[x] P15 Controlled Modelling Access
[x] P18 Test Coverage Hardening
[x] P19 Provenance
[ ] Airflow DAG ETL
[ ] 10-Year Historical Backfill
[ ] P16 Wet-Day Definition
[ ] P17 Feature Contribution
[ ] Wet-Day Clustering
```

---

# 3. P18 — Test Coverage Hardening

P18 tidak menambah fitur meteorologis baru. Tujuannya adalah memperkuat test pada boundary yang akan menjadi kritis saat pipeline digunakan untuk retry, rerun, historical backfill, dan orchestration dengan Airflow.

Hasil verifikasi terbaru:

```text
61 passed
```

Area yang diuji mencakup:

- request validation dan deterministic request key;
- CDS normalization;
- UTC/local time preservation;
- ERA5-Land accumulation formula;
- unit conversion;
- missing variable dan incomplete local-day rejection;
- temporal/spatial validation;
- RAW dan Parquet checkpoint;
- overwrite refusal;
- missing checkpoint failure;
- SHA-256 checksum;
- Parquet provenance metadata;
- RAW checksum change detection;
- PostgreSQL schema/DDL;
- metadata dan observation UPSERT;
- NaN -> NULL;
- runtime fresh/resume path;
- artifact handoff;
- P15 repository negative cases dan deterministic ordering.

P18 penting sebelum Airflow karena Airflow akan menggunakan retry, rerun, backfill, task restart, dan partial failure recovery. Pipeline harus sudah terbukti aman ketika artifact atau database state sebelumnya sudah tersedia.

---

# 4. Konsep Checkpoint

Checkpoint adalah artifact yang disimpan setelah satu tahap penting berhasil.

Pipeline saat ini memiliki dua checkpoint utama:

```text
RAW checkpoint         = NetCDF
Transformed checkpoint = Parquet
```

Tujuannya adalah agar pipeline tidak harus mengulang seluruh proses dari awal.

Contoh:

```text
CDS download
   |
   v
RAW NetCDF
```

Jika RAW sudah berhasil dibuat, download CDS tidak perlu dilakukan lagi saat pipeline dijalankan ulang.

Demikian juga:

```text
RAW
 |
 v
Transform
 |
 v
Parquet
```

Jika Parquet sudah berhasil dibuat dan masih valid terhadap RAW, transform tidak perlu dilakukan lagi.

---

# 5. RAW Checkpoint

RAW checkpoint menggunakan format NetCDF.

Contoh path:

```text
data/reanalysis-era5-land/raw/cds/2022/
2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
```

RAW dianggap sebagai scientific source truth setelah data selesai diunduh dari CDS.

RAW bersifat immutable. Pipeline tidak boleh menimpa RAW existing secara diam-diam.

Jika file RAW yang sesuai logical request sudah ada, pipeline melakukan resume.

---

# 6. Transformed Checkpoint

Transformed checkpoint menggunakan Parquet.

Contoh:

```text
data/reanalysis-era5-land/transformed/cds/2022/
2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

Parquet berisi hasil daily climate setelah:

```text
hourly ERA5-Land
        |
        v
UTC+7 local-day aggregation
        |
        v
unit conversion
        |
        v
daily climate
```

Parquet juga tidak boleh ditimpa secara diam-diam. Jika sudah tersedia, pipeline mencoba resume setelah provenance/checksum diperiksa.

---

# 7. Status Checkpoint

Checkpoint memiliki dua status normal:

```text
created
resumed
```

## 7.1 `created`

Artinya artifact belum ada, pipeline menjalankan tahap terkait, lalu membuat artifact baru.

Contoh RAW:

```text
CDS download
-> NetCDF ditulis
-> status = created
```

Contoh transformed:

```text
RAW -> transform
-> Parquet ditulis
-> status = created
```

## 7.2 `resumed`

Artinya artifact sudah ada dan pipeline menggunakan kembali artifact tersebut tanpa mengulang pekerjaan mahal.

RAW resume:

```text
NetCDF sudah ada
-> CDS tidak di-download ulang
-> RAW dibaca kembali
-> RAW tetap divalidasi
```

Transformed resume:

```text
Parquet sudah ada
-> transform tidak dijalankan ulang
-> provenance/checksum diperiksa
-> Parquet dibaca kembali
-> transformed data tetap divalidasi
```

---

# 8. Resume Bukan Berarti Melewati Validasi

Ini adalah kontrak penting pipeline.

Walaupun checkpoint di-resume, validation tetap dijalankan.

RAW resume:

```text
RAW NetCDF existing
        |
        v
load RAW
        |
        v
RAW temporal validation
        |
        v
RAW spatial validation
        |
        v
RAW schema validation
```

Transformed resume:

```text
Parquet existing
        |
        v
check provenance
        |
        v
check RAW checksum
        |
        v
load Parquet
        |
        v
transformed temporal validation
        |
        v
transformed spatial validation
        |
        v
DataFrame validation
```

Prinsipnya:

```text
file exists != file valid
```

Artifact existing tidak boleh dipercaya hanya karena file ada.

---

# 9. Fresh Run vs Resume Run

Fresh run terbaru:

```text
RAW checkpoint         = created
Transformed checkpoint = created
Database               = created / loaded
```

Durasi sekitar:

```text
16 detik
```

Run berikutnya dengan request yang sama:

```text
RAW checkpoint         = resumed
Transformed checkpoint = resumed
Database               = existing + UPSERT
```

Durasi sekitar:

```text
0.53 detik
```

Ini menunjukkan checkpoint memberi manfaat nyata.

---

# 10. Status Lain di Pipeline

Selain `created` dan `resumed`, ada nilai status lain dengan konteks berbeda.

```text
Schema/Table : ready
Pipeline     : completed
DB load mode : idempotent upsert
```

`ready` berarti struktur database tersedia.

`completed` berarti seluruh flow EXTRACT -> TRANSFORM -> LOAD DATABASE selesai tanpa error.

`idempotent upsert` adalah mode persistence, bukan status checkpoint.

---

# 11. Checkpoint Error State

Pipeline tidak menggunakan status normal seperti:

```text
failed
invalid
stale
checksum_mismatch
```

Jika terjadi masalah, pipeline melempar exception dan berhenti.

Contoh:

```text
RAW sha256 sekarang
!=
raw_sha256 di Parquet
```

maka transformed checkpoint tidak di-resume.

Pendekatan ini sengaja dipilih agar `resumed` hanya berarti artifact memang aman digunakan.

---

# 12. P19 — Provenance

P19 menambahkan traceability artifact.

Tujuannya menjawab pertanyaan operasional:

```text
kapan data diunduh?
Parquet ini berasal dari RAW mana?
apakah RAW berubah?
Parquet mana yang digunakan database?
apakah Parquet berubah?
```

P19 dibuat minimal. Tidak digunakan:

```text
transform_version
Git commit SHA
daily_climate_raw table
```

---

# 13. Provenance Chain

Struktur provenance saat ini:

```text
RAW NetCDF
    |
    | downloaded_at
    | SHA-256 dihitung dari bytes
    v
Transformed Parquet
    |
    | raw_artifact_path
    | raw_sha256
    | downloaded_at
    | transformed_at
    v
PostgreSQL daily_climate
    |
    | downloaded_at
    | transformed_artifact_path
    | transformed_sha256
    | transformed_at
    v
daily_climate_detail
```

---

# 14. RAW Metadata

RAW NetCDF menyimpan:

```text
downloaded_at
```

Nilai ini menjawab kapan source data benar-benar selesai diperoleh.

Ia tidak berubah pada resume.

Prinsip:

```text
downloaded_at
!=
waktu pipeline terakhir dijalankan
```

---

# 15. RAW SHA-256

SHA-256 dihitung dari bytes file RAW.

Prinsip:

```text
same filename + same hash
= same bytes

same filename + different hash
= file berubah
```

RAW SHA-256 tidak disimpan di PostgreSQL. Ia disimpan pada metadata Parquet.

Alasannya: PostgreSQL dibangun dari transformed artifact, bukan langsung dari RAW.

---

# 16. Metadata Parquet

Parquet menyimpan metadata:

```text
raw_artifact_path
raw_sha256
downloaded_at
transformed_at
```

Metadata ini tidak menjadi kolom observation.

Data row tetap:

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

Provenance berada pada file-level metadata agar tidak diulang pada setiap observation row.

---

# 17. PostgreSQL Provenance

`daily_climate` menyimpan:

```text
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

Schema konseptual:

```text
daily_climate
--------------------------------
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
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

`daily_climate_detail` tetap fokus pada observation meteorologis.

---

# 18. Mengapa RAW Path/SHA Tidak Masuk PostgreSQL

Database tidak memerlukan direct RAW reference karena provenance chain sudah tersedia:

```text
PostgreSQL
        |
        | transformed_artifact_path
        | transformed_sha256
        v
Parquet
        |
        | raw_artifact_path
        | raw_sha256
        v
RAW NetCDF
```

Dengan demikian provenance tetap lengkap tanpa menambah field yang tidak perlu.

---

# 19. Transformed SHA-256

Database menyimpan:

```text
transformed_sha256
```

Contoh runtime terbaru:

```text
271efd40cf56c6d3ea33859c9b60fe32497ec205657ac01d0a60414d11857112
```

Artinya database dapat mengetahui exact Parquet bytes yang digunakan sebagai sumber partition tersebut.

Jika:

```text
same path + different sha256
```

maka transformed artifact berubah.

---

# 20. Resume Provenance Check

Pada transformed resume:

1. `raw_artifact_path` di metadata Parquet dibandingkan dengan RAW artifact saat ini.
2. `raw_sha256` di metadata Parquet dibandingkan dengan SHA-256 RAW saat ini.
3. Jika cocok, resume diizinkan.
4. Jika berbeda, resume ditolak dan transformed checkpoint harus dibuat ulang.

Ini mencegah kondisi:

```text
RAW berubah
tetapi Parquet lama tetap digunakan
```

---

# 21. Idempotent Database Load

Metadata database menggunakan:

```text
request_key
```

UPSERT:

```text
ON CONFLICT (request_key)
DO UPDATE
```

Observation identity:

```text
daily_climate_id
+ valid_time
+ latitude
+ longitude
```

Observation juga menggunakan UPSERT.

Karena itu rerun menghasilkan:

```text
same daily_climate_id
same observation identity
no duplicate rows
```

---

# 22. Hasil Runtime Resume Terbaru

Fresh run:

```text
rows_processed   = 747
daily_climate_id = 1
```

Run kedua:

```text
RAW checkpoint         = resumed
Transformed checkpoint = resumed
daily_climate_id       = 1
rows_processed         = 747
```

Provenance tetap sama:

```text
downloaded_at
2026-09-18 05:51:01.656520+00:00

transformed_sha256
271efd40cf56c6d3ea33859c9b60fe32497ec205657ac01d0a60414d11857112
```

Ini membuktikan checkpoint resume, provenance validation, dan idempotent database load bekerja bersama dengan benar.

---

# 23. Monthly Partition

Pipeline menggunakan monthly partition melalui:

```text
run_monthly_batch()
split_request_by_month()
```

Untuk 10 tahun, secara normal akan terbentuk sekitar:

```text
120 monthly partitions
```

Setiap monthly partition memiliki:

```text
request_key sendiri
RAW checkpoint sendiri
Parquet checkpoint sendiri
daily_climate row sendiri
```

Ini akan sangat berguna untuk Airflow karena retry dapat dilakukan per partition/bulan.

---

# 24. ERA5-Land Local-Day Context

ERA5-Land daily aggregation membutuhkan source context sebelum target period.

Contoh target:

```text
2022-01-01 -> 2022-01-03
```

RAW retrieval mencakup:

```text
2021-12-31 -> 2022-01-03
```

Hari sebelumnya dibutuhkan karena:

```text
00:00 WIB = 17:00 UTC hari sebelumnya
```

serta accumulation formula membutuhkan boundary dari hari sebelumnya.

Ini bukan additional modelling partition, tetapi source context untuk daily aggregation.

---

# 25. Checkpoint Decision Flow

```text
START
  |
  v
RAW file exists?
  |
  +-- NO --> download CDS
  |           |
  |           v
  |        save RAW
  |           |
  |           v
  |        status=created
  |
  +-- YES --> load RAW
              |
              v
           status=resumed

        |
        v
validate RAW

        |
        v
Parquet exists?
  |
  +-- NO --> transform
  |           |
  |           v
  |        save Parquet
  |           |
  |           v
  |        status=created
  |
  +-- YES --> read provenance
              |
              v
          RAW checksum match?
              |
              +-- NO --> ERROR
              |
              +-- YES --> load Parquet
                          |
                          v
                       status=resumed

        |
        v
validate transformed

        |
        v
UPSERT PostgreSQL

        |
        v
COMPLETED
```

---

# 26. Checkpoint dan Historical Backfill

Historical backfill 10 tahun dapat memerlukan banyak request ke CDS.

Tanpa checkpoint, kegagalan pada bagian akhir proses dapat memaksa pipeline mengulang download dan transform yang sebenarnya sudah selesai.

Dengan monthly checkpoint:

```text
bulan yang sudah selesai = resumed
bulan yang belum selesai = created
```

Airflow nantinya dapat melanjutkan partition yang belum selesai tanpa mengulang semua pekerjaan.

---

# 27. Checkpoint dan Airflow Retry

Contoh:

```text
extract berhasil
transform berhasil
database load gagal
```

Pada retry:

```text
RAW      -> resumed
Parquet  -> resumed
DB load  -> retried
```

Jadi:

```text
retry != full reprocessing
```

Ini adalah salah satu alasan utama checkpoint, P18, dan P19 diselesaikan sebelum DAG dibuat.

---

# 28. Arsitektur Tanggung Jawab

Core pipeline tetap memiliki scientific/business logic:

```text
pipeline/
    extraction
    validation
    transform
    storage
    checksum
    provenance
    database load
    repository read
```

Airflow nanti hanya mengatur:

```text
schedule
partition interval
task dependency
retry
concurrency
pool
alerting
artifact reference handoff
```

Airflow tidak boleh mengambil alih scientific transformation.

---

# 29. Kontrak yang Harus Dipertahankan

RAW:

```text
immutable
validated after resume
downloaded_at preserved
```

Parquet:

```text
not silently overwritten
contains RAW provenance metadata
validated after resume
RAW checksum verified before resume
```

Database:

```text
request_key idempotency
observation UPSERT
missing != zero
transformed artifact provenance stored
```

Time:

```text
valid_time retained
valid_time_local retained
UTC+7 local day
```

Modelling:

```text
P15 read-only
no wet-day label yet
no final feature selection yet
```

---

# 30. Status Roadmap Setelah P19

```text
[x] P4  RAW hourly retained
[x] P5  UTC+7 local day
[x] P6  variable-specific aggregation
[x] P7  observation schema whitelist
[x] P8  CDS metadata excluded
[x] P9  provider naming
[x] P10 checkpoint / resume
[x] P11 idempotent DB load
[x] P12 temporal validation
[x] P13 spatial validation
[x] P14 unit conversion
[x] P15 controlled modelling access
[x] P18 test coverage hardening
[x] P19 provenance
[x] P20 hourly ERA5-Land RAW

[ ] Airflow DAG ETL
[ ] 10-Year Historical Backfill
[ ] P16 Wet-Day Definition
[ ] P17 Feature Contribution
[ ] Wet-Day Clustering
```

---

# 31. Bukti Verifikasi

Unit test:

```text
61 passed
```

Fresh runtime:

```text
RAW checkpoint         = created
Transformed checkpoint = created
rows                   = 747
daily_climate_id       = 1
```

Resume runtime:

```text
RAW checkpoint         = resumed
Transformed checkpoint = resumed
rows                   = 747
daily_climate_id       = 1
duration                ~0.53 s
```

---

# 32. Kesimpulan

Sebelum masuk ke Airflow, pipeline sudah memiliki tiga fondasi penting:

```text
scientific correctness
safe retry/resume
artifact provenance
```

Flow sekarang:

```text
CDS
 ↓
RAW NetCDF
 + downloaded_at
 + SHA-256
 ↓
validated daily transform
 ↓
Parquet
 + raw_artifact_path
 + raw_sha256
 + downloaded_at
 + transformed_at
 ↓
PostgreSQL
 + transformed_artifact_path
 + transformed_sha256
 ↓
P15 controlled read
```

Pipeline sekarang berada pada kondisi yang tepat untuk masuk ke tahap berikutnya:

```text
Airflow DAG ETL
```

Airflow nantinya tidak perlu mengubah scientific logic. Ia hanya akan mengorkestrasi boundary yang sudah terbukti bekerja:

```text
extract_or_resume
transform_or_resume
load
```

---

## File Utama yang Berkaitan

```text
pipeline/model.py
pipeline/interfaces.py
pipeline/storage.py
pipeline/runtime.py
pipeline/database.py
pipeline/load.py
pipeline/transform.py
pipeline/validation.py
pipeline/daily_climate_repository.py

tests/unit/test_storage.py
tests/unit/test_database.py
tests/unit/test_load.py
tests/unit/test_runtime.py
tests/unit/test_transform.py
tests/unit/test_validation.py
tests/unit/test_daily_climate_repository.py

Readme-pipeline.md
AGENTS.md
tests/AGENTS.md
tests/README_TESTING.md
```
