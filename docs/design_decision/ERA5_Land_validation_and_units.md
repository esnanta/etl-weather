# Dokumentasi Validasi Temporal, Validasi Spatial, dan Konversi Unit ERA5-Land

## Status

```text
[x] P12 Temporal validation
[x] P13 Spatial validation
[x] P14 Unit conversion
```

Dokumen ini merangkum tiga tahap validasi dan transformasi penting pada pipeline ERA5-Land sebelum data digunakan untuk membentuk fitur hari basah dan pola musim.

---

# P12 — Temporal Validation

## Tujuan

P12 memastikan bahwa dimensi waktu pada data ERA5-Land benar, lengkap, tidak duplikat, dan konsisten antara waktu UTC dan waktu lokal UTC+7.

ERA5-Land menggunakan UTC sebagai referensi waktu, sedangkan analisis lokal menggunakan UTC+7.

Pipeline menyimpan:

```text
valid_time
valid_time_local
```

dengan arti:

```text
valid_time        = waktu UTC
valid_time_local  = waktu lokal UTC+7
```

Contoh:

```text
valid_time        = 2021-12-31 17:00:00
valid_time_local  = 2022-01-01 00:00:00
```

Keduanya menunjukkan momen yang sama.

## Validasi pada data RAW

Data RAW harus memenuhi kontrak berikut:

- `valid_time` tersedia;
- tipe datanya datetime;
- timestamp unik;
- timestamp berurutan;
- interval antar timestamp adalah 1 jam;
- tidak ada timestep yang hilang.

Pada run terbaru:

```text
valid_time_start    = 2021-12-31 00:00:00
valid_time_end      = 2022-01-03 23:00:00
unique_time_steps   = 96
expected_time_steps = 96
time_interval       = 1 jam
missing_time_steps  = 0
```

Artinya data hourly lengkap selama 4 hari:

```text
4 hari × 24 jam = 96 timestep
```

## Validasi pada data harian

Setelah transformasi harian, pipeline memastikan:

```text
valid_time_local = valid_time + 7 jam
```

dan:

```text
valid_time        = 17:00 UTC
valid_time_local  = 00:00 UTC+7
```

Pada run terbaru:

```text
valid_time_utc_start   = 2021-12-31 17:00:00
valid_time_utc_end     = 2022-01-02 17:00:00
valid_time_local_start = 2022-01-01 00:00:00
valid_time_local_end   = 2022-01-03 00:00:00
utc_to_local_offset    = 7 jam
local_days             = 3
expected_local_days    = 3
duplicate_grid_records = 0
```

Artinya tiga hari lokal tersedia lengkap dan tidak memiliki record grid duplikat.

---

# P13 — Spatial Validation

## Tujuan

P13 memastikan bahwa struktur spasial ERA5-Land benar dan konsisten dengan:

- bounding box yang diminta;
- resolusi horizontal dataset;
- grid reguler latitude–longitude;
- anggota grid valid yang stabil dari hari ke hari.

ERA5-Land menggunakan grid reguler dengan resolusi:

```text
0.1°
```

## Bounding box yang digunakan

Untuk run ini:

```text
latitude  : 3.4 sampai 5.2
longitude : 96.1 sampai 97.6
```

Dengan resolusi 0,1°, pipeline mendapatkan:

```text
latitude_count  = 19
longitude_count = 16
```

sehingga total grid RAW adalah:

```text
19 × 16 = 304 grid
```

## Apa maksud “grid RAW konsisten dengan bbox dan resolusi ERA5-Land 0,1°”?

Artinya koordinat yang diterima dari ERA5-Land benar-benar membentuk grid reguler sesuai area yang diminta.

Latitude bergerak seperti:

```text
3.4
3.5
3.6
3.7
...
5.2
```

Longitude bergerak seperti:

```text
96.1
96.2
96.3
96.4
...
97.6
```

Selisih antar titik adalah:

```text
0.1°
```

Itulah arti resolusi horizontal ERA5-Land 0,1°.

Untuk latitude:

```text
5.2 - 3.4 = 1.8°
1.8 / 0.1 = 18 interval
18 + 1 = 19 titik
```

Untuk longitude:

```text
97.6 - 96.1 = 1.5°
1.5 / 0.1 = 15 interval
15 + 1 = 16 titik
```

Maka:

```text
19 × 16 = 304 titik grid
```

Ketika pipeline melaporkan:

```text
latitude_count            = 19
expected_latitude_count   = 19
longitude_count           = 16
expected_longitude_count  = 16
grid_points               = 304
```

artinya grid yang diterima benar-benar sesuai dengan bbox dan resolusi yang diharapkan.

## Mengapa transformed hanya 249 grid, bukan 304?

RAW memiliki semua titik grid geometris di dalam bounding box:

```text
304 grid
```

Namun ERA5-Land adalah dataset land-surface. Sebagian grid berada di laut atau area tanpa nilai land-surface valid, sehingga nilainya dapat berupa `NaN`.

Setelah transformasi, grid tanpa data valid tidak masuk ke dataset harian final.

Hasilnya:

```text
valid_land_grid_cells = 249
```

Ini bukan kehilangan data secara acak. Artinya dari 304 titik geometris pada bounding box, terdapat 249 grid land-valid yang dipakai pada data harian.

## Konsistensi grid antarhari

Pipeline memastikan set grid valid tidak berubah dari hari ke hari.

Pada run terbaru:

```text
local_days             = 3
valid_land_grid_cells  = 249
grid_cells_per_day_min = 249
grid_cells_per_day_max = 249
grid_membership_stable = True
```

Artinya:

```text
2022-01-01 → 249 grid
2022-01-02 → 249 grid
2022-01-03 → 249 grid
```

Bukan hanya jumlahnya sama; pasangan `(latitude, longitude)` juga identik untuk setiap hari.

Jika salah satu hari kehilangan satu grid atau muncul grid baru yang tidak ada pada hari lain, validasi spatial akan gagal.

---

# P14 — Unit Conversion

## Tujuan

P14 memastikan unit transformed dataset lebih mudah dipakai untuk analisis pertanian dan pemodelan hari basah.

RAW tetap disimpan dalam unit asli ERA5-Land. Konversi dilakukan pada data transformed.

## Unit hasil transformasi

| Variabel | Unit ERA5-Land | Unit hasil transformasi |
|---|---|---|
| `total_precipitation` | m | mm/day |
| `2m_temperature` | K | °C |
| `2m_dewpoint_temperature` | K | °C |
| `volumetric_soil_water_layer_1` | m³/m³ | tetap m³/m³ |
| `surface_solar_radiation_downwards` | J/m² | tetap J/m²/day |

## `total_precipitation`

ERA5-Land menyimpan curah hujan dalam meter:

```text
m
```

Pipeline mengubahnya menjadi:

```text
mm/day
```

dengan:

```text
mm = m × 1000
```

Contoh:

```text
0.047809685 m × 1000 = 47.809685 mm/day
```

Alasan:

- millimeter adalah unit umum curah hujan;
- lebih mudah dibandingkan dengan data stasiun;
- cocok untuk threshold hari basah;
- lebih intuitif untuk analisis pertanian.

## `2m_temperature`

Unit sumber:

```text
Kelvin
```

Konversi:

```text
°C = K - 273.15
```

Contoh:

```text
294.532233 K - 273.15 = 21.382233 °C
```

Alasan:

- Celsius umum digunakan di Indonesia;
- lebih mudah ditafsirkan untuk kondisi tanaman;
- lebih intuitif dalam visualisasi dan laporan.

## `2m_dewpoint_temperature`

Unit sumber:

```text
Kelvin
```

Konversi:

```text
°C = K - 273.15
```

Alasan: dew point lebih mudah dibandingkan langsung dengan temperatur udara jika keduanya memakai unit yang sama.

## `volumetric_soil_water_layer_1`

Unit:

```text
m³/m³
```

Unit ini dipertahankan karena sudah merupakan unit standar volumetric soil moisture dan langsung sesuai untuk analisis kelembaban tanah.

## `surface_solar_radiation_downwards`

Unit sumber:

```text
J/m²
```

Setelah agregasi harian, interpretasinya menjadi:

```text
J/m²/day
```

Unit ini dipertahankan karena:

- tetap dekat dengan unit asli ERA5-Land;
- belum ada kebutuhan langsung untuk mengubahnya;
- menghindari transformasi tambahan yang belum diperlukan;
- dapat dikonversi ke MJ/m²/day nanti jika dibutuhkan.

---

# Hubungan P12, P13, dan P14

```text
P12
Apakah waktunya benar?
        ↓
P13
Apakah lokasi/grid-nya benar?
        ↓
P14
Apakah unit nilainya benar?
```

Jika ketiganya lulus, dataset memiliki tiga fondasi:

```text
temporal correctness
spatial correctness
unit correctness
```

---

# Ringkasan hasil run

```text
RAW Temporal Validation PASSED
RAW Spatial Validation PASSED
ERA5-Land Temporal Validation PASSED
ERA5-Land Spatial Validation PASSED
DataFrame Validation PASSED
```

Data RAW:

```text
96 timestep hourly
19 latitude
16 longitude
304 grid geometris
```

Data transformed:

```text
3 hari lokal
249 grid land-valid per hari
747 baris total
```

Perhitungannya:

```text
249 × 3 = 747
```

Dengan:

```text
grid_membership_stable = True
```

artinya set lokasi grid yang digunakan konsisten untuk seluruh hari.

---

# Status akhir

```text
[x] P12 Temporal validation
[x] P13 Spatial validation
[x] P14 Unit conversion
```

Dataset sekarang siap masuk ke tahap berikutnya:

```text
P15 — Daily feature dataset
P16 — Wet-day definition
```
