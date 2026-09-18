# Keputusan Desain Pipeline ERA5-Land

## 1. Tujuan dan ruang lingkup

Dokumen ini menjelaskan keputusan ilmiah dan teknis yang benar-benar diterapkan oleh `pipeline/` dan `scripts/` untuk mengolah **ERA5-Land hourly data** dari Copernicus Climate Data Store (CDS) menjadi observasi harian. Penjelasan ditujukan untuk developer lintas bidang: pembaca tidak harus memahami seluruh detail ERA5-Land atau struktur internal kode sebelum membaca dokumen ini.

Ruang lingkup dokumen adalah jalur:

```text
CDS ERA5-Land per jam
    -> NetCDF mentah
    -> agregasi hari lokal UTC+7
    -> Parquet harian
    -> PostgreSQL
    -> DataFrame hanya-baca untuk analisis
```

Repository juga memiliki jalur ARCO ERA5. Jalur tersebut memakai beberapa komponen umum, tetapi tidak boleh dianggap memiliki kontrak ilmiah yang sama dengan ERA5-Land. Pada implementasi saat ini, transformasi harian khusus hanya dijalankan ketika nama dataset adalah `reanalysis-era5-land`. Dataset lain hanya melalui pemilihan kolom observasi.

Dokumen ini tidak menyatakan hasil pengujian dan belum menyelaraskan isi dengan `tests/`. Pembahasan pengujian dilakukan secara terpisah.

---

## 2. Dasar ilmiah pemilihan data

ERA5-Land adalah hasil reanalysis permukaan daratan yang dibuat dengan menjalankan ulang komponen daratan ERA5 pada resolusi yang lebih tinggi. Produk pada CDS tersedia pada grid latitude-longitude reguler 0,1° × 0,1°, sementara resolusi native-nya sekitar 9 km. Data hourly dipilih sebagai sumber utama karena pipeline perlu membentuk hari menurut waktu lokal Aceh, bukan hanya menurut hari UTC.

Keputusan menggunakan data hourly mempunyai dua alasan:

1. rata-rata temperatur, titik embun, dan kelembapan tanah harus dihitung dari seluruh keadaan per jam dalam hari lokal; dan
2. total presipitasi serta radiasi harus dibentuk ulang pada batas hari UTC+7 sesuai konvensi akumulasi ERA5-Land.

Produk daily statistics dari CDS memang dapat menghasilkan statistik harian pada zona waktu tertentu, tetapi pipeline mempertahankan data hourly sebagai sumber kebenaran ilmiah. Dengan demikian, batas hari, kelengkapan sampel, konversi satuan, dan keterlacakan hasil tetap dapat diperiksa di dalam sistem ini.

### 2.1 Variabel yang didukung

Jalur CDS ERA5-Land saat ini menerima lima kode variabel dari penyedia:

| Kode | Nama ERA5-Land | Sifat temporal | Statistik keluaran |
|---|---|---|---|
| `tp` | total precipitation | akumulasi sejak 00:00 UTC | total hari lokal |
| `ssrd` | surface solar radiation downwards | akumulasi sejak 00:00 UTC | total hari lokal |
| `t2m` | 2 metre temperature | keadaan sesaat | rata-rata hari lokal |
| `d2m` | 2 metre dewpoint temperature | keadaan sesaat | rata-rata hari lokal |
| `swvl1` | volumetric soil water layer 1 | keadaan sesaat | rata-rata hari lokal |

Kode pendek asli dipertahankan pada NetCDF, DataFrame transformasi, dan Parquet. Nama panjang seperti `total_precipitation` hanya digunakan ketika membentuk permintaan CDS, kemudian dinormalisasi kembali menjadi `tp` setelah data dibuka.

Alasan keputusan ini adalah keterlacakan. Developer dapat membandingkan variabel dengan metadata ERA5-Land tanpa menghadapi nama internal baru yang maknanya mungkin berbeda. Nama kolom yang lebih deskriptif baru digunakan pada PostgreSQL, setelah statistik dan satuannya sudah pasti.

---

## 3. Peta implementasi

| Bagian | Tanggung jawab saat ini |
|---|---|
| `pipeline/utils.py` | validasi dan normalisasi permintaan, profil dataset, pembagian bulanan, `request_key` |
| `pipeline/extract.py` | permintaan CDS, konteks hari sebelumnya, normalisasi nama, ekstraksi spasial dan temporal |
| `pipeline/validation.py` | validasi struktur, waktu, grid mentah, serta keluaran harian |
| `pipeline/transform.py` | pembentukan hari UTC+7, agregasi per jenis variabel, konversi satuan |
| `pipeline/storage.py` | jalur artefak, NetCDF, Parquet, checksum, dan resume |
| `pipeline/load.py` | pemetaan kolom dan pemuatan idempoten ke PostgreSQL |
| `pipeline/database.py` | schema, tabel, indeks unik, dan hubungan antartabel |
| `pipeline/daily_climate_repository.py` | pembacaan observasi harian dari PostgreSQL untuk analisis |
| `pipeline/runtime.py` | urutan extract, transform, dan load tanpa ketergantungan pada Airflow |
| `scripts/config.py` | contoh konfigurasi periode, variabel, dan bounding box |
| `scripts/run_pipeline_cds.py` | entry point lokal jalur CDS |
| `scripts/read_daily_climate.py` | contoh pembacaan hasil PostgreSQL ke DataFrame |

`pipeline/model.py`, `pipeline/interfaces.py`, dan `pipeline/logging.py` menyediakan struktur data, batas antarkomponen, serta pencatatan metrik. Log membantu pemeriksaan operasional, tetapi bukan pengganti validasi ilmiah.

---

## 4. Permintaan data dan partisi bulanan

Satu permintaan dinyatakan oleh:

```text
source, dataset, start_date, end_date, variables, bounding box
```

Contoh yang saat ini terdapat di `scripts/config.py`:

```python
WeatherRequest(
    source=DataSource.CDS,
    dataset="reanalysis-era5-land",
    start_date="2022-01-01",
    end_date="2022-01-03",
    variables=["tp", "t2m", "swvl1", "ssrd", "d2m"],
    bbox=BoundingBox(
        north_lat=5.2,
        south_lat=3.4,
        east_lon=97.6,
        west_lon=96.1,
    ),
)
```

Permintaan dinormalisasi sebelum diproses: nama dataset dan variabel diubah ke huruf kecil, kode variabel diperiksa terhadap daftar yang didukung, variabel duplikat ditolak, tanggal harus berformat `YYYY-MM-DD`, dan bounding box harus mempunyai urutan koordinat yang valid.

Periode panjang dibagi menurut **bulan kalender** oleh `run_monthly_batch()`. Misalnya, 20 Januari–10 Maret menjadi tiga partisi:

```text
20–31 Januari
1–28/29 Februari
1–10 Maret
```

Pembagian bulanan dipilih karena CDS menerima permintaan yang lebih mudah dikendalikan per bulan dan karena kegagalan satu partisi tidak memaksa seluruh periode historis dihitung ulang. Fungsi bernama `run_weekly_batch()` masih tersedia untuk kompatibilitas, tetapi pada kode saat ini fungsi tersebut hanya meneruskan pemrosesan ke strategi bulanan.

---

## 5. Ekstraksi hourly dari CDS

Untuk setiap partisi, pipeline meminta:

- seluruh jam `00:00` sampai `23:00` UTC;
- semua variabel yang dipilih;
- area `[north, west, south, east]`;
- format NetCDF tanpa arsip tambahan.

Walaupun perhitungan `tp` dan `ssrd` hanya memerlukan nilai pada batas akumulasi tertentu, semua jam tetap diunduh. Keputusan ini menghasilkan satu artefak hourly yang juga dapat digunakan untuk variabel keadaan, memungkinkan validasi kontinuitas setiap jam, dan menjaga bentuk sumber yang seragam jika daftar variabel berubah.

### 5.1 Konteks satu hari sebelum periode

Untuk ERA5-Land, tanggal awal pengambilan dimundurkan satu hari:

```text
periode logis       : 2022-01-01 sampai 2022-01-03
periode yang diambil: 2021-12-31 sampai 2022-01-03
```

Hari tambahan diperlukan karena 1 Januari pukul 00:00 UTC+7 sama dengan 31 Desember pukul 17:00 UTC. Selain itu, akumulasi `tp` dan `ssrd` pada awal hari lokal memerlukan nilai akumulasi dari hari UTC sebelumnya.

NetCDF tetap dinamai menurut periode logis 1–3 Januari, tetapi isinya mencakup 31 Desember sebagai konteks perhitungan. Setelah transformasi, hasil difilter kembali agar hanya memuat 1–3 Januari waktu lokal.

ERA5-Land dimulai pada 1 Januari 1950. Kode tidak akan meminta tanggal lebih awal dari batas tersebut. Konsekuensinya, hari lokal 1 Januari 1950 pada UTC+7 tidak memiliki konteks hari sebelumnya yang cukup dan tidak dapat dibentuk lengkap oleh aturan ini.

---

## 6. Representasi waktu

### 6.1 Waktu pada data mentah

`valid_time` pada NetCDF adalah waktu valid dari CDS dalam UTC. Dokumentasi ERA5-Land menjelaskan bahwa parameter keadaan berlaku pada timestamp tersebut, sedangkan parameter akumulasi berasal dari short forecast yang dimulai setiap hari pada 00:00 UTC.

Pipeline tidak mengganti `valid_time` mentah dengan tanggal lokal karena timestamp UTC diperlukan untuk memahami tepatnya nilai dari penyedia.

### 6.2 Waktu pada data harian

Keluaran harian mempunyai dua kolom:

```text
valid_time
valid_time_local
```

Keduanya menyatakan awal hari lokal yang sama:

```text
valid_time       = waktu UTC awal hari lokal
valid_time_local = tanggal lokal pada pukul 00:00
```

Hubungannya:

```text
valid_time_local = valid_time + 7 jam
```

Contoh:

```text
valid_time       : 2021-12-31 17:00:00
valid_time_local : 2022-01-01 00:00:00
```

Kolom tersebut sengaja disimpan sebagai dua timestamp tanpa informasi zona waktu pada DataFrame dan tabel saat ini. Makna zonanya ditetapkan oleh kontrak: `valid_time` dibaca sebagai UTC dan `valid_time_local` sebagai Asia/Jakarta (UTC+7). Karena itu, nama kolom tidak boleh diganti menjadi `date` yang ambigu.

Hari lokal berlabel `D` mencakup interval:

```text
[D-1 17:00 UTC, D 17:00 UTC)
```

Untuk sampel keadaan hourly, timestamp yang masuk adalah `D-1 17:00` sampai `D 16:00` UTC, setara dengan pukul 00:00 sampai 23:00 waktu lokal.

---

## 7. Agregasi variabel keadaan

Untuk `t2m`, `d2m`, dan `swvl1`, pipeline menambahkan offset tujuh jam pada setiap timestamp, membulatkannya ke awal tanggal lokal, lalu menghitung mean per kombinasi:

```text
hari lokal + latitude + longitude
```

Secara matematis:

```text
x_mean(D, i, j) = (1 / 24) * sum(x_h(i, j)), h = 0..23 waktu lokal
```

Mean hanya diterbitkan jika tepat 24 nilai non-null tersedia untuk variabel, hari, dan grid tersebut. `groupby.count()` tidak menghitung `NaN`; akibatnya, satu nilai keadaan yang hilang membuat hasil variabel itu untuk grid-hari tersebut tidak dibentuk.

Alasan aturan 24 sampel adalah mencegah rata-rata parsial dianggap mewakili satu hari penuh. Misalnya, mean temperatur dari 18 jam yang kebetulan lebih banyak berasal dari malam hari dapat menghasilkan bias terhadap mean harian sebenarnya.

Jika beberapa variabel diminta sekaligus, hasil harian setiap variabel digabung dengan irisan yang sama. Sebuah grid-hari hanya masuk ke keluaran akhir jika tersedia pada seluruh hasil variabel yang digabungkan.

---

## 8. Agregasi `tp` dan `ssrd`

### 8.1 Mengapa nilai hourly tidak dijumlahkan

Menurut dokumentasi ERA5-Land, parameter akumulasi berasal dari short forecast harian yang dimulai pada 00:00 UTC. Nilai pada suatu jam adalah akumulasi dari 00:00 UTC sampai jam tersebut. Nilai pada `00:00 UTC` merupakan akumulasi 24 jam yang berakhir pada timestamp itu, sehingga sebenarnya mewakili hari UTC sebelumnya.

Karena sifat kumulatif tersebut, menjumlahkan 24 nilai akan menghitung periode awal hari berulang kali. Pipeline menggunakan selisih antar-batas akumulasi, sesuai panduan ECMWF untuk daily accumulation pada zona waktu non-UTC.

### 8.2 Rumus hari lokal UTC+7

Midnight lokal terjadi pada 17:00 UTC. Untuk hari lokal `D`, kode menerapkan:

```text
Total(D) = A(D 17:00) + A(D 00:00) - A(D-1 17:00)
```

dengan `A(t)` adalah nilai akumulasi ERA5-Land pada timestamp `t`.

Makna setiap bagian:

- `A(D 00:00) - A(D-1 17:00)` memberikan akumulasi dari pukul 17:00 sampai 24:00 UTC pada hari sebelumnya;
- `A(D 17:00)` memberikan akumulasi dari 00:00 sampai 17:00 UTC pada hari `D`;
- penjumlahan keduanya menghasilkan interval 24 jam dari tengah malam lokal sampai tengah malam lokal berikutnya.

Contoh numerik untuk 2 Januari waktu lokal:

```text
A(1 Jan 17:00) = 8 mm
A(2 Jan 00:00) = 10 mm
A(2 Jan 17:00) = 6 mm

Total(2 Jan lokal) = 6 + 10 - 8 = 8 mm
```

Nilai 8 mm tersebut mencakup 1 Januari 17:00 UTC sampai 2 Januari 17:00 UTC, yaitu 2 Januari 00:00–24:00 UTC+7.

Rumus yang sama digunakan untuk `ssrd`, karena kedua variabel memiliki konvensi akumulasi ERA5-Land yang sama. Kode hanya membentuk baris jika tiga nilai batas yang diperlukan dapat dipasangkan pada grid yang sama.

Nilai hasil yang lebih kecil dari `-1e-10` dicatat sebagai peringatan tetapi tetap dipertahankan. Keputusan ini mencegah koreksi diam-diam terhadap data sumber. Pipeline saat ini belum menerapkan clipping ke nol atau aturan kualitas berbasis rentang fisik.

---

## 9. Konversi satuan

Konversi dilakukan setelah agregasi. NetCDF mentah tetap memakai nilai dan satuan dari penyedia, sedangkan Parquet dan PostgreSQL memakai satuan analisis berikut:

| Variabel | Operasi | Satuan keluaran |
|---|---|---|
| `tp` | nilai harian × 1000 | mm/day |
| `t2m` | mean harian − 273,15 | °C |
| `d2m` | mean harian − 273,15 | °C |
| `swvl1` | tidak diubah | m³/m³ |
| `ssrd` | tidak diubah | J/m²/day |

Konversi presipitasi dari meter ke milimeter mengikuti definisi hydrological accumulation ERA5-Land. Temperatur dikonversi dari Kelvin ke Celsius agar mudah digunakan pada analisis pertanian. `ssrd` sengaja tidak diubah menjadi W/m² atau MJ/m²/day karena kolom database menyatakan total energi harian; konversi tambahan harus dilakukan secara eksplisit oleh analisis yang memerlukannya.

---

## 10. Bentuk data hasil transformasi

Kolom hasil transformasi adalah:

```text
valid_time
valid_time_local
latitude
longitude
<variabel yang diminta>
```

Urutan variabel mengikuti urutan pada permintaan. Untuk konfigurasi CDS di `scripts/config.py`, bentuknya adalah:

```text
valid_time, valid_time_local, latitude, longitude,
tp, t2m, swvl1, ssrd, d2m
```

Kolom metadata CDS seperti `number` atau `expver` tidak disertakan sebagai observasi cuaca. Pemilihan kolom secara eksplisit mencegah perubahan metadata dari penyedia masuk tanpa sengaja ke kontrak data hilir.

Baris diurutkan berdasarkan `valid_time_local`, `latitude`, lalu `longitude`. Urutan deterministik memudahkan perbandingan hasil dan pembacaan ulang, tetapi identitas ilmiah baris tetap ditentukan oleh waktu dan koordinat, bukan oleh nomor urut DataFrame.

---

## 11. Validasi yang diterapkan

### 11.1 Validasi data mentah

Setelah unduhan baru maupun resume, NetCDF harus memenuhi kondisi berikut:

- berupa `xarray.Dataset`;
- mempunyai koordinat `valid_time`, `latitude`, dan `longitude`;
- memuat seluruh variabel yang diminta;
- `valid_time` bertipe datetime, unik, menaik, dan berinterval tepat satu jam;
- latitude dan longitude tidak duplikat, monoton, dan berjarak 0,1°;
- semua koordinat berada di dalam bounding box;
- jumlah koordinat pada setiap sumbu sesuai rentang inklusif bounding box dan resolusi 0,1°.

Validasi jumlah sumbu mengasumsikan batas permintaan sejajar dengan grid 0,1°. Sebagai contoh, rentang latitude 3,4–5,2 menghasilkan 19 posisi, sedangkan longitude 96,1–97,6 menghasilkan 16 posisi; grid geometrisnya berjumlah `19 × 16 = 304`.

Pemeriksaan ini memastikan struktur ruang-waktu sumber utuh sebelum transformasi. Kode saat ini belum memvalidasi rentang fisik setiap variabel, misalnya batas temperatur atau presipitasi maksimum.

### 11.2 Validasi data harian

Keluaran ERA5-Land harus memenuhi:

- kolom waktu, koordinat, dan seluruh variabel permintaan tersedia;
- `valid_time_local - valid_time` selalu tujuh jam;
- `valid_time_local` selalu pukul 00:00:00;
- `valid_time` selalu pukul 17:00:00 UTC secara semantik;
- tidak ada duplikat `valid_time_local + latitude + longitude`;
- setiap tanggal lokal dari awal sampai akhir periode tersedia;
- koordinat berada dalam bounding box dan sejajar dengan grid 0,1°;
- himpunan pasangan latitude-longitude identik pada setiap hari.

Jumlah grid hasil harian dapat lebih kecil daripada grid geometris mentah. Penyebabnya antara lain titik yang tidak mempunyai 24 nilai keadaan non-null atau titik yang tidak valid untuk permukaan daratan. Yang dipersyaratkan adalah himpunan grid hasil tersebut stabil dari hari ke hari.

### 11.3 Nilai hilang

Pipeline tidak mengganti nilai hilang dengan nol. Pada saat pemuatan database:

```text
NaN -> NULL
```

Perbedaan ini penting: `tp = 0` menyatakan tidak ada presipitasi, sedangkan `tp = NULL` menyatakan nilai tidak tersedia. Imputasi, interpolasi, atau definisi hari basah tidak dilakukan pada lapisan ETL ini.

Perlu dicatat bahwa perlakuan saat agregasi bergantung pada jenis variabel. Untuk variabel keadaan, kurang dari 24 nilai non-null menyebabkan grid-hari tidak diterbitkan. Untuk variabel akumulasi, operasi aritmetika mempertahankan `NaN` jika salah satu nilai batasnya `NaN`; nilai tersebut kemudian disimpan sebagai `NULL` apabila barisnya tetap lolos penggabungan dan validasi grid.

---

## 12. Artefak, resume, dan keterlacakan

### 12.1 Dua lapisan artefak

```text
RAW         = NetCDF hourly
TRANSFORMED = Parquet harian
```

NetCDF adalah sumber ilmiah lokal yang tidak boleh ditimpa. Parquet adalah hasil deterministik dari proses agregasi dan konversi yang dapat ditelusuri ke NetCDF tertentu.

Penyimpanan dilakukan melalui file sementara pada direktori yang sama, kemudian dipindahkan secara atomik ke nama akhir. Pendekatan ini mengurangi kemungkinan file akhir terlihat lengkap ketika proses penulisan sebenarnya terhenti.

Jika file RAW atau Parquet sudah ada, runtime melakukan resume dan memvalidasinya kembali. Fungsi penyimpanan sendiri menolak overwrite. Dengan demikian:

```text
file ada -> baca ulang -> periksa provenance -> validasi -> lanjut
```

bukan:

```text
file ada -> anggap benar
```

### 12.2 Jalur artefak saat ini

Jalur dibentuk dari dataset, lapisan, sumber, tahun tanggal awal, periode, dan signature variabel yang telah diurutkan. Contoh:

```text
data/reanalysis-era5-land/raw/cds/2022/
  2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.nc

data/reanalysis-era5-land/transformed/cds/2022/
  2022_0101_0103_d2m_ssrd_swvl1_t2m_tp.parquet
```

Pengurutan signature membuat `['tp', 't2m']` dan `['t2m', 'tp']` menunjuk nama partisi yang sama karena himpunan observasinya sama.

**Batasan implementasi saat ini:** nama dan jalur artefak belum memasukkan bounding box. Dua permintaan dengan dataset, sumber, periode, dan variabel sama tetapi area berbeda akan menunjuk jalur file yang sama. Sementara desain ini belum diubah, satu ruang penyimpanan tidak boleh dipakai untuk kedua bounding box tersebut tanpa memisahkan `DATA_DIRECTORY` atau membersihkan artefak secara sadar. Berbeda dengan jalur file, `request_key` PostgreSQL sudah memasukkan bounding box.

### 12.3 Provenance

NetCDF menyimpan `downloaded_at` pada atribut file. Parquet menyimpan:

```text
raw_artifact_path
raw_sha256
downloaded_at
transformed_at
```

Saat Parquet di-resume, pipeline menghitung ulang SHA-256 NetCDF dan membandingkannya dengan metadata Parquet. Perubahan isi atau pergantian sumber RAW menyebabkan resume ditolak.

PostgreSQL menyimpan:

```text
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

Checksum dan jalur RAW tidak diulang di PostgreSQL karena hubungan ke RAW sudah dicatat pada metadata Parquet. Rantainya adalah:

```text
baris PostgreSQL
    -> Parquet dan SHA-256 Parquet
        -> NetCDF dan SHA-256 NetCDF
```

Provenance saat ini belum mencatat versi kode transformasi atau commit Git. Karena itu, checksum membuktikan identitas artefak, tetapi belum cukup untuk merekonstruksi versi algoritme jika kode berubah di masa depan.

---

## 13. Identitas logis dan idempotensi PostgreSQL

`request_key` adalah SHA-256 dari representasi terurut yang memuat:

```text
source, dataset, start_date, end_date,
daftar variabel terurut, serta seluruh batas bounding box
```

Kunci ini mengidentifikasi satu partisi logis tanpa bergantung pada urutan variabel. PostgreSQL memakai schema `climate_data` dengan dua tabel.

### 13.1 `daily_climate`

Satu baris menyimpan metadata satu permintaan:

```text
id, request_key, dataset_name, source,
start_date, end_date, variable_signature,
north_lat, south_lat, east_lon, west_lon,
downloaded_at, transformed_artifact_path,
transformed_sha256, transformed_at
```

`request_key` mempunyai indeks unik. Jika partisi yang sama dimuat ulang, metadata diperbarui dan `id` yang sama dikembalikan.

### 13.2 `daily_climate_detail`

Satu baris menyimpan satu observasi grid-hari:

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

Kolom variabel bersifat nullable agar nilai hilang tetap berbeda dari nol. Kunci unik observasi adalah:

```text
daily_climate_id + valid_time + latitude + longitude
```

Pemuatan menggunakan UPSERT dalam kelompok 1.000 baris. Pengulangan permintaan memperbarui observasi dengan identitas yang sama, bukan menambah duplikat. Seluruh metadata dan detail dimuat di dalam satu transaksi database sehingga kegagalan tidak meninggalkan partisi setengah selesai.

Database dan tabel dibuat jika belum tersedia. Perilaku pembuatan ini merupakan kemudahan lingkungan pengembangan; deployment produksi tetap harus mengelola kredensial dan perubahan schema secara terkendali.

---

## 14. Akses hanya-baca untuk analisis

`load_daily_climate_for_modelling()` membaca PostgreSQL berdasarkan pilihan eksplisit:

```text
source
dataset_name
start_date dan end_date
bounding box yang sama persis
```

Rentang tanggal menggunakan batas awal inklusif dan batas akhir eksklusif pada hari setelah `end_date`. Hasil diurutkan menurut hari lokal, latitude, longitude, dan `daily_climate_id`.

Sebelum dikembalikan, DataFrame diperiksa agar:

- seluruh kolom kanonis tersedia;
- hasil tidak kosong;
- tidak ada duplikat hari lokal dan grid, termasuk duplikat lintas partisi;
- `valid_time_local` tepat pada tengah malam;
- tanggal tidak keluar dari periode yang diminta.

Lapisan ini sengaja hanya-baca. Ia tidak membuat tabel, tidak mengganti nilai hilang, tidak melakukan scaling, tidak menghitung anomali, dan tidak memberi label hari basah. Pemisahan ini menjaga agar dataset iklim kanonis tidak berubah mengikuti eksperimen model.

Contoh pemakaian terdapat pada `scripts/read_daily_climate.py`:

```python
selection = DailyClimateSelection(
    source=DataSource.CDS,
    dataset_name="reanalysis-era5-land",
    start_date=date(2022, 1, 1),
    end_date=date(2022, 1, 3),
    bbox=ACEH_LAND_DOMINANT_BBOX,
)

dataframe = load_daily_climate_for_modelling(selection)
```

---

## 15. Alur eksekusi lokal

Lingkungan dibuat dengan:

```bash
./scripts/setup_environment.sh
```

Skrip tersebut membuat virtual environment `myvenv` jika belum ada dan memasang `requirements.txt`. Konfigurasi CDS memerlukan `CDSAPI_KEY`, sedangkan PostgreSQL memerlukan `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, dan `POSTGRES_PASSWORD`.

Pipeline CDS dijalankan dengan:

```bash
python scripts/run_pipeline_cds.py
```

Urutan di dalam `WeatherPipeline` adalah:

1. tentukan jalur RAW;
2. unduh atau resume NetCDF;
3. validasi NetCDF;
4. ubah NetCDF menjadi DataFrame;
5. transformasikan atau resume Parquet;
6. validasi hasil harian;
7. UPSERT metadata dan observasi ke PostgreSQL.

Data hasil dapat dibaca kembali dengan:

```bash
python scripts/read_daily_climate.py
```

Skrip konfigurasi saat ini berfungsi sebagai contoh kode, bukan antarmuka baris perintah umum. Periode, daftar variabel, dan bounding box diubah melalui konstanta pada `scripts/config.py`.

---

## 16. Batas orkestrasi

Semua aturan yang mengubah makna data berada di `pipeline/`, termasuk interpretasi timestamp, agregasi, konversi satuan, validasi, checkpoint, provenance, dan pemuatan database. Runtime lokal menyusun tahap-tahap tersebut melalui antarmuka extractor, storage, transformer, dan loader.

Jika Airflow digunakan kemudian, Airflow hanya mengatur jadwal, interval pekerjaan, ketergantungan, retry, concurrency, credential, dan pemantauan. Logika ilmiah tidak dipindahkan ke DAG. Alasan pemisahan ini adalah hasil numerik untuk permintaan yang sama harus tetap identik, baik pipeline dipanggil dari skrip lokal maupun dari orkestrator.

---

## 17. Batasan yang harus diketahui developer

Dokumentasi ini membedakan keputusan yang sudah diterapkan dari kemampuan yang belum tersedia:

1. Jalur artefak belum membedakan bounding box; `request_key` database sudah membedakannya.
2. Provenance belum mencatat versi algoritme atau commit Git.
3. Validasi memeriksa struktur, kontinuitas waktu, dan konsistensi grid, tetapi belum memeriksa rentang fisik setiap variabel.
4. Nilai akumulasi negatif hanya diberi peringatan dan tidak dikoreksi.
5. UTC+7 tidak dapat membentuk hari lokal pertama ERA5-Land, 1 Januari 1950, karena konteks hari sebelumnya tidak tersedia.
6. Timestamp harian disimpan tanpa informasi zona; interpretasi UTC dan UTC+7 bergantung pada kontrak kolom.
7. Konfigurasi skrip masih berupa konstanta dan belum menjadi CLI parametrik.
8. Jalur ARCO tetap terpisah dan tidak boleh dianggap menghasilkan agregasi harian ERA5-Land.

Daftar ini penting agar developer tidak menyimpulkan jaminan yang lebih luas daripada perilaku kode saat ini.

---

## 18. Prinsip yang dipertahankan

1. NetCDF hourly adalah sumber kebenaran ilmiah lokal.
2. Hari analisis mengikuti kalender Aceh UTC+7.
3. Timestamp UTC dan label hari lokal dipertahankan bersama.
4. Variabel keadaan dan akumulasi tidak diproses dengan rumus yang sama.
5. Hari dengan mean keadaan harus mempunyai 24 sampel non-null.
6. `tp` dan `ssrd` dihitung dari batas akumulasi ERA5-Land, bukan dari penjumlahan 24 nilai kumulatif.
7. Satuan sumber dipertahankan pada RAW; satuan analisis diterapkan setelah agregasi.
8. Nilai hilang tidak diubah menjadi nol.
9. Artefak yang di-resume tetap harus melewati pemeriksaan provenance dan validasi.
10. RAW tidak ditimpa; pemuatan PostgreSQL bersifat idempoten.
11. Data iklim kanonis dipisahkan dari transformasi khusus model.
12. Orkestrator tidak menentukan makna ilmiah data.

---

## 19. Referensi ilmiah utama

Keputusan temporal dan spasial di atas mengikuti dokumentasi resmi berikut:

- [ERA5-Land hourly data from 1950 to present](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land)
- [ERA5-Land: data documentation](https://confluence.ecmwf.int/display/CKB/ERA5-Land%3A+data+documentation)
- [ERA5 family post-processed daily statistics documentation](https://confluence.ecmwf.int/display/CKB/ERA5+family+post-processed+daily+statistics+documentation)
- [ERA5 terminology: analysis and forecast; time and steps](https://confluence.ecmwf.int/display/CKB/ERA5+terminology%3A+analysis+and+forecast%3B+time+and+steps%3B+instantaneous+and+accumulated+and+mean+rates+and+min%2Fmax+parameters)

Referensi tersebut menjadi dasar bagi keputusan berikut:

- ERA5-Land pada CDS memakai grid reguler 0,1° × 0,1° dan tersedia hourly;
- parameter keadaan berlaku pada timestamp, sedangkan parameter akumulasi berasal dari forecast;
- forecast ERA5-Land untuk akumulasi dimulai pada 00:00 UTC dan berjalan sampai 24 jam;
- nilai akumulasi bertimestamp 00:00 UTC mewakili 24 jam sebelumnya;
- daily accumulation pada zona non-UTC harus disusun dari bagian akumulasi di kedua sisi midnight UTC.

Jika dokumentasi penyedia, kode, dan dokumen ini berbeda, perubahan tidak boleh dilakukan secara diam-diam. Perbedaannya harus diperiksa terhadap data contoh, lalu kontrak ilmiah dan implementasi diperbarui bersama.
