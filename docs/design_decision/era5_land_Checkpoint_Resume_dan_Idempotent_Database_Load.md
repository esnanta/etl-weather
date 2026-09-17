# Checkpoint/Resume dan Idempotent Database Load

## Konteks

Pipeline ETL cuaca ini dibangun sebagai komponen pemrosesan data yang nantinya akan diorkestrasi oleh Apache Airflow. Karena itu, desain P10 dan P11 tidak memasukkan logika scheduler, DAG, retry policy, sensor, concurrency, atau data interval langsung ke dalam modul ETL.

Prinsip yang dipakai adalah:

> **Airflow mengatur kapan dan dalam urutan apa pipeline dijalankan; pipeline menentukan bagaimana data diekstrak, divalidasi, ditransformasi, disimpan, dan dimuat dengan aman.**

Dengan pemisahan ini, pipeline tetap dapat dijalankan secara lokal melalui `scripts/run_pipeline_cds.py`, dapat diuji dengan `pytest`, dan nanti dapat dipanggil oleh task Airflow tanpa memindahkan business logic ke dalam DAG.

Status akhir:

```text
[x] P10 checkpoint / resume
[x] P11 idempotent database load
```

---

# P10 — Checkpoint / Resume

## 1. Tujuan

P10 bertujuan membuat setiap tahap pipeline dapat dilanjutkan dari hasil yang sudah berhasil dibuat sebelumnya.

Masalah yang ingin diselesaikan:

```text
EXTRACT berhasil
TRANSFORM berhasil
LOAD gagal
```

Tanpa mekanisme resume, retry akan mengulang:

```text
download ulang
→ menulis RAW ulang
→ transform ulang
→ load ulang
```

Perilaku tersebut tidak efisien dan berisiko, terutama karena:

- CDS merupakan sumber eksternal;
- proses ekstraksi bisa mahal dan lambat;
- RAW harus diperlakukan sebagai source of truth;
- retry dari Airflow seharusnya menjadi mekanisme recovery, bukan membuat pekerjaan yang sama berulang-ulang.

## 2. Keputusan Desain

### 2.1 Checkpoint diberi nama secara deterministik

Artifact RAW dan transformed tidak menggunakan nama berdasarkan waktu eksekusi atau `run_id`.

Nama file dibangun dari:

```text
periode + variable signature
```

Contoh:

```text
2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

Struktur penyimpanan:

```text
data/
└── reanalysis-era5-land/
    ├── raw/
    │   └── cds/
    │       └── 2022/
    │           └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc
    └── transformed/
        └── cds/
            └── 2022/
                └── 2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

### Alasan

Artifact merepresentasikan **partition data logis**, bukan satu eksekusi program.

Dengan demikian:

```text
request sama
→ path sama
→ partition logis sama
```

Hal ini penting untuk retry, backfill, rerun manual, Airflow data interval, dan reproducibility.

Jika nama file menggunakan `run_id`, timestamp runtime, atau UUID, retry akan membuat salinan baru dan pipeline tidak dapat mengetahui bahwa partition yang sama sudah selesai diproses.

## 3. RAW Bersifat Immutable

RAW NetCDF diperlakukan sebagai source of truth.

Keputusan:

```text
RAW existing
→ jangan overwrite
```

Jika pipeline akan menulis RAW ke path yang sudah ada melalui jalur normal penulisan, operasi ditolak.

### Alasan

RAW adalah representasi terdekat dengan data provider. Menimpa RAW dapat menyebabkan kehilangan lineage, kesulitan membandingkan hasil, perubahan diam-diam pada input, dan debugging yang tidak dapat direproduksi.

Karena itu, RAW yang sudah berhasil dibuat harus digunakan kembali melalui mekanisme resume.

## 4. Atomic Write untuk RAW

Penulisan RAW dilakukan secara atomic.

Secara konseptual:

```text
write temporary file
→ selesai dengan sukses
→ rename ke path final
```

### Alasan

Masalah yang ingin dihindari:

```text
proses mati di tengah penulisan
→ file final sudah ada
→ retry mengira checkpoint valid
```

Dengan atomic write, path final hanya muncul setelah file selesai ditulis.

Ini penting untuk lingkungan Airflow karena task dapat gagal akibat worker restart, container termination, network problem, disk issue, atau process kill.

Checkpoint harus berarti **artifact valid**, bukan hanya “ada file dengan nama tersebut”.

## 5. Boundary Pipeline

Pipeline sekarang memiliki boundary yang secara konseptual dapat dipetakan langsung ke task Airflow:

```text
extract_or_resume()
        ↓
transform_or_resume()
        ↓
load()
```

### 5.1 `extract_or_resume()`

Perilaku:

```text
RAW belum ada
→ extract dari provider
→ validasi
→ simpan RAW

RAW sudah ada
→ load RAW checkpoint
→ validasi ulang
→ lanjut
```

### 5.2 `transform_or_resume()`

Perilaku:

```text
Parquet belum ada
→ transform dari RAW
→ validasi
→ simpan Parquet

Parquet sudah ada
→ load Parquet
→ validasi ulang
→ lanjut
```

### 5.3 `load()`

`load()` tidak menentukan retry sendiri. Ia hanya melakukan persistence secara idempotent.

Retry policy nanti menjadi tanggung jawab Airflow.

## 6. Resume Tetap Melakukan Validasi

Keputusan penting:

```text
artifact exists
≠ otomatis dipercaya
```

Pada resume:

- RAW tetap melalui temporal validation;
- RAW tetap melalui spatial validation;
- transformed Parquet tetap melalui temporal validation;
- transformed Parquet tetap melalui spatial validation;
- transformed Parquet tetap melalui schema/DataFrame validation.

### Alasan

File bisa ada tetapi corrupt, tidak lengkap, berasal dari implementasi lama, salah schema, atau tidak sesuai request.

Resume yang aman harus berarti:

```text
artifact ditemukan
+ kontrak data masih valid
→ artifact boleh digunakan
```

## 7. Bukti Runtime P10

Run pertama:

```text
RAW checkpoint
status = created

Transformed checkpoint
status = created
```

Durasi:

```text
18.56 detik
```

Run kedua dengan request yang sama:

```text
RAW checkpoint
status = resumed

Transformed checkpoint
status = resumed
```

Durasi:

```text
0.67 detik
```

Pada run kedua tidak dilakukan download ulang CDS.

Ini menunjukkan bahwa checkpoint benar-benar digunakan sebagai mekanisme recovery/resume.

## 8. Bukti Unit Test P10

Test yang relevan:

```text
test_raw_artifact_path_is_deterministic
test_save_raw_netcdf_refuses_to_overwrite_existing_checkpoint
test_load_raw_netcdf_resumes_existing_checkpoint
```

Seluruhnya lulus.

---

# P11 — Idempotent Database Load

## 9. Tujuan

P11 memastikan bahwa menjalankan partition yang sama lebih dari satu kali tidak membuat data duplikat di PostgreSQL.

Masalah sebelumnya:

```text
run pertama
→ INSERT metadata
→ INSERT 747 observation

run kedua
→ INSERT metadata baru
→ INSERT 747 observation lagi
```

Akibatnya retry dapat menciptakan duplikasi data.

Dalam Airflow, task harus aman untuk dijalankan ulang. Karena itu database load harus idempotent.

## 10. Identitas Partition Menggunakan `request_key`

Setiap request logis dibuatkan hash deterministik:

```text
request_key
```

Identitas tersebut dibangun dari:

```text
source
dataset
start_date
end_date
variable set
bbox
```

### Alasan

Hanya menggunakan `dataset + start_date + end_date` tidak cukup.

Contohnya, periode sama tetapi bbox berbeda, atau periode sama tetapi variable set berbeda, merupakan partition logis yang berbeda.

Karena itu request key harus merepresentasikan seluruh identitas input yang menentukan isi dataset.

## 11. Urutan Variabel Tidak Mengubah Identitas

Contoh:

```text
tp, t2m, d2m
```

dan:

```text
d2m, tp, t2m
```

menghasilkan identitas partition yang sama.

### Alasan

Urutan list bukan bagian dari makna ilmiah request. Yang relevan adalah himpunan variabel yang diminta.

Jika urutan ikut menentukan key, perubahan urutan konfigurasi yang tidak bermakna dapat membuat partition baru dan memicu duplikasi.

## 12. Idempotensi Metadata

Tabel:

```text
climate_data.weather_dataset
```

menggunakan:

```text
request_key
```

sebagai idempotency key.

Perilaku:

```text
request_key belum ada
→ INSERT metadata
→ dataset_id baru

request_key sudah ada
→ gunakan dataset_id yang sama
```

Bukti runtime:

Run pertama:

```text
dataset_id = 1
request_key = e3eadef...
```

Run kedua:

```text
dataset_id = 1
request_key = e3eadef...
```

Tidak dibuat `dataset_id = 2`.

## 13. Idempotensi Observation

Untuk detail data, conflict key yang digunakan adalah:

```text
dataset_id
+ valid_time
+ latitude
+ longitude
```

Secara konseptual:

```text
ON CONFLICT (...)
DO UPDATE
```

bukan append buta.

### Alasan

Satu observation harian didefinisikan oleh:

```text
partition dataset
+ waktu
+ grid cell
```

Jika kombinasi tersebut sudah ada, retry harus memperbarui row yang sama, bukan menambah row baru.

## 14. Mengapa Menggunakan UPSERT

Alternatif yang tidak dipilih:

### Delete lalu insert ulang

```text
DELETE partition
→ INSERT ulang
```

Tidak dipilih sebagai default karena lebih destruktif, menciptakan window tanpa data, retry di tengah proses lebih berisiko, dan lebih sulit jika nanti ada referensi downstream.

### Append lalu deduplicate kemudian

Tidak dipilih karena database dapat berada dalam state duplikat, downstream dapat membaca data sebelum deduplication, dan bertentangan dengan prinsip idempotensi.

### UPSERT

Dipilih karena:

```text
same logical observation
→ same database row
```

Ini paling sesuai untuk retry Airflow.

## 15. `rows_processed` Bukan `rows_inserted`

Log diubah dari:

```text
rows_inserted
```

menjadi:

```text
rows_processed
```

### Alasan

Pada run kedua:

```text
rows_processed = 747
```

tidak berarti 747 row baru dibuat.

Artinya 747 observation masuk ke proses UPSERT.

Istilah `rows_inserted` akan menyesatkan karena sebagian atau seluruh row bisa merupakan update terhadap row yang sudah ada.

## 16. Kolom Database Tetap Representatif terhadap Hasil Transformasi

Variable utama pipeline tetap:

```text
tp
t2m
d2m
swvl1
ssrd
```

Namun PostgreSQL menyimpan hasil daily statistics dengan nama kolom eksplisit:

```text
tp_daily_total_mm
t2m_daily_mean_c
d2m_daily_mean_c
swvl1_daily_mean_m3_m3
ssrd_daily_total_j_m2
```

Mapping:

```text
tp    → tp_daily_total_mm
t2m   → t2m_daily_mean_c
d2m   → d2m_daily_mean_c
swvl1 → swvl1_daily_mean_m3_m3
ssrd  → ssrd_daily_total_j_m2
```

### Alasan

Nama provider dipertahankan sebagai identitas variable utama.

Di database, statistik dan unit dibuat eksplisit agar pembaca tidak salah menginterpretasikan nilai.

Contoh:

```text
tp
```

pada RAW merupakan variable total precipitation dari provider.

Sedangkan:

```text
tp_daily_total_mm
```

secara eksplisit berarti hasil agregasi daily total dengan unit millimeter.

## 17. Migrasi Metadata dan Schema Lama

P11 mempertimbangkan database yang sudah pernah dibuat sebelum `request_key` tersedia.

Keputusan:

- schema baru memiliki `request_key`;
- metadata lama yang masih sesuai dapat diadopsi;
- observation lama dibersihkan dari duplikasi sebelum constraint idempotensi diberlakukan;
- kolom lama hasil eksperimen naming dimigrasikan ke kolom daily statistics.

### Alasan

Upgrade pipeline sebaiknya tidak memaksa database dibuang hanya karena kontrak baru ditambahkan.

Namun compatibility hanya dipakai sebagai mekanisme migrasi, bukan untuk mempertahankan dua schema aktif secara permanen.

## 18. Bukti Runtime P11

Run pertama:

```text
Dataset metadata upsert

dataset_id         = 1
request_key        = e3eadef...
variable_signature = d2m_ssrd_swvl1_t2m_tp
```

Observation:

```text
rows_processed = 747
mode = idempotent upsert
```

Run kedua:

```text
dataset_id     = 1
rows_processed = 747
mode           = idempotent upsert
```

Artinya partition logis tetap menggunakan dataset metadata yang sama.

## 19. Verifikasi Duplikasi Database

Query:

```sql
SELECT
    dataset_id,
    valid_time,
    latitude,
    longitude,
    COUNT(*)
FROM climate_data.weather_dataset_detail
GROUP BY
    dataset_id,
    valid_time,
    latitude,
    longitude
HAVING COUNT(*) > 1;
```

Hasil:

```text
0 rows
```

Query tersebut dijalankan lebih dari sekali dan tetap menghasilkan nol duplikat.

Ini menjadi bukti runtime bahwa rerun tidak membuat observation duplicate.

## 20. Bukti Unit Test P11

Test yang relevan:

```text
test_build_request_key_is_stable_when_variable_order_changes
test_build_request_key_changes_when_partition_identity_changes
test_prepare_observation_dataframe_uses_daily_database_columns
test_insert_dataset_reuses_existing_request_key_dataset_id
```

Seluruh test lulus.

---

# Hubungan dengan Airflow

## 21. Yang Sudah Menjadi Tanggung Jawab Pipeline

Pipeline sekarang bertanggung jawab terhadap:

```text
deterministic artifact identity
RAW immutability
atomic checkpoint write
resume RAW
resume transformed Parquet
data validation
deterministic request identity
database metadata idempotency
observation UPSERT
scientific transformation
unit conversion
```

Semua behavior tersebut dapat diuji tanpa Airflow.

## 22. Yang Sengaja Tidak Dibangun di Pipeline

Hal berikut tidak dimasukkan ke modul ETL:

```text
DAG schedule
Airflow retry count
retry delay
data_interval_start
data_interval_end
task dependency
Airflow pools
max_active_runs
task concurrency
sensors
XCom implementation
Airflow Connections
alerting
SLA
```

### Alasan

Semua hal tersebut merupakan concern orkestrasi.

Jika logic itu dimasukkan ke `pipeline/`, maka pipeline sulit diuji tanpa Airflow, business logic menjadi terikat scheduler, local execution menjadi lebih sulit, dan DAG serta pipeline memiliki tanggung jawab tumpang tindih.

## 23. Bentuk Integrasi Airflow yang Diinginkan

Secara konseptual DAG nantinya cukup seperti:

```text
extract_or_resume
        ↓
transform_or_resume
        ↓
load
```

Airflow membawa parameter partition, misalnya dari:

```text
data_interval_start
data_interval_end
```

Task hanya meneruskan request/artifact reference ke pipeline.

XCom nantinya sebaiknya membawa metadata kecil seperti artifact path, dataset_id, request_key, row count, atau partition dates; bukan DataFrame atau dataset NetCDF.

## 24. Retry Scenario

### Kasus A — Extract gagal sebelum checkpoint selesai

```text
RAW final belum ada
→ retry melakukan extract ulang
```

### Kasus B — Extract sudah selesai, Transform gagal

```text
RAW sudah ada
→ retry resume RAW
→ transform ulang
```

### Kasus C — Transform selesai, Load gagal

```text
RAW ada
Parquet ada
→ retry resume kedua artifact
→ hanya load yang perlu melakukan pekerjaan persistence
```

### Kasus D — Load sebenarnya berhasil tetapi task dianggap gagal

Misalnya worker kehilangan koneksi setelah transaction selesai.

Retry berikutnya:

```text
request_key sama
dataset_id sama
observation conflict key sama
→ UPSERT
→ tidak ada duplikasi
```

Kasus D merupakan alasan utama P11 diperlukan sebelum Airflow digunakan.

---

# Hasil Verifikasi Keseluruhan

## 25. Pytest

Hasil test suite:

```text
31 passed
3 warnings
```

Warning berasal dari:

```text
xarray / netCDF4 / NumPy
DeprecationWarning
```

Warning tersebut tidak berasal dari logic checkpoint, resume, atau database idempotency.

Tidak ada test failure.

## 26. Runtime

Run pertama:

```text
RAW        = created
Parquet    = created
dataset_id = 1
rows_processed = 747
duration   = 18.56 s
```

Run kedua:

```text
RAW        = resumed
Parquet    = resumed
dataset_id = 1
rows_processed = 747
duration   = 0.67 s
```

Database duplicate check:

```text
0 rows
```

---

# Keputusan Akhir

## P10

```text
[x] P10 checkpoint / resume
```

Keputusan dianggap selesai karena:

- artifact path deterministik;
- RAW immutable;
- atomic write;
- RAW dapat di-resume;
- Parquet dapat di-resume;
- resumed artifact tetap divalidasi;
- retry tidak perlu mengulang network extraction;
- unit test dan runtime test berhasil.

## P11

```text
[x] P11 idempotent database load
```

Keputusan dianggap selesai karena:

- request memiliki identity deterministik;
- urutan variable tidak mengubah request identity;
- metadata partition menggunakan `request_key`;
- rerun menggunakan `dataset_id` yang sama;
- observation menggunakan UPSERT;
- conflict key mewakili satu observation grid-waktu;
- terminology log memakai `rows_processed`;
- database duplicate query menghasilkan `0 rows`;
- unit test dan runtime test berhasil.

---

# Implikasi untuk Tahap Berikutnya

Dengan P10 dan P11 selesai, pipeline sekarang memiliki fondasi operasional yang lebih aman untuk:

```text
historical backfill
multi-year processing
Airflow retry
Airflow catchup
partial failure recovery
reproducible processing
```

Tahap berikutnya dapat masuk ke:

```text
P15 — Daily Feature Dataset
```

tanpa harus kembali mengubah dasar retry dan persistence.

Namun ketika Airflow mulai diimplementasikan, DAG tetap harus mengikuti prinsip:

> **Airflow mengorkestrasi pipeline; Airflow tidak mengambil alih business logic pipeline.**
