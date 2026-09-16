# Dokumentasi P12 — Validasi Temporal dan P14 — Konversi Unit ERA5-Land

## Tujuan

Dokumen ini menjelaskan dua tahap penting pada pipeline ERA5-Land:

- **P12 — Validasi Temporal**
- **P14 — Konversi Unit**

Kedua tahap ini diperlukan agar data harian yang masuk ke Parquet dan database tidak hanya benar secara teknis, tetapi juga benar secara ilmiah sebelum digunakan untuk pemodelan hari basah.

---

# P12 — Validasi Temporal

## Mengapa validasi temporal diperlukan?

ERA5-Land menggunakan waktu **UTC**, sedangkan analisis pertanian pada proyek ini menggunakan waktu lokal **UTC+7**.

Pipeline menyimpan dua kolom waktu:

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

Kedua timestamp tersebut menunjukkan momen yang sama.

Karena UTC+7 berada tujuh jam lebih maju daripada UTC:

```text
valid_time_local = valid_time + 7 jam
```

---

## Validasi pada data RAW

Data RAW ERA5-Land dipertahankan dalam resolusi hourly.

Validasi temporal pada layer RAW memastikan bahwa:

1. `valid_time` tersedia.
2. `valid_time` menggunakan tipe datetime.
3. Timestamp berurutan secara kronologis.
4. Tidak terdapat timestamp yang duplikat.
5. Interval antar-timestamp adalah 1 jam.
6. Tidak terdapat timestep yang hilang pada rentang yang seharusnya kontinu.

Secara ideal:

```text
00:00
01:00
02:00
03:00
...
23:00
```

dengan interval:

```text
1 jam
```

Validasi ini penting karena data hourly menjadi dasar perhitungan agregasi harian.

Jika satu atau beberapa jam hilang, maka rata-rata suhu, kelembaban tanah, dew point, maupun perhitungan akumulasi harian dapat menjadi tidak lengkap.

---

## Validasi pada hasil harian

Setelah data hourly diagregasi menjadi data harian, pipeline melakukan validasi terhadap hubungan antara `valid_time` dan `valid_time_local`.

Untuk UTC+7:

```text
valid_time_local = valid_time + 7 jam
```

Karena awal hari lokal adalah pukul:

```text
00:00 UTC+7
```

maka timestamp UTC yang bersesuaian adalah:

```text
17:00 UTC pada hari sebelumnya
```

Contoh:

```text
2021-12-31 17:00 UTC
        =
2022-01-01 00:00 UTC+7
```

Pipeline memastikan bahwa:

- `valid_time` berada pada pukul `17:00 UTC`;
- `valid_time_local` berada pada pukul `00:00`;
- selisih keduanya tepat 7 jam;
- semua tanggal lokal yang diminta tersedia;
- tidak ada duplikasi untuk kombinasi:

```text
valid_time_local
latitude
longitude
```

Dengan demikian setiap grid hanya boleh mempunyai satu record untuk satu hari lokal.

---

## Mengapa validasi ini penting untuk hari basah?

Hari basah merupakan konsep berbasis hari kalender lokal.

Sebagai contoh, hujan yang terjadi:

```text
31 Desember 2021 18:00 UTC
```

secara lokal adalah:

```text
1 Januari 2022 01:00 UTC+7
```

Sehingga hujan tersebut harus masuk ke:

```text
1 Januari 2022
```

bukan:

```text
31 Desember 2021
```

Jika batas waktu UTC dan lokal tidak divalidasi dengan benar, klasifikasi hari basah dapat bergeser satu hari dan mengubah pola musim yang dianalisis.

---

# P14 — Konversi Unit

## Prinsip utama

RAW ERA5-Land disimpan dalam unit asli dari sumber.

Konversi unit hanya dilakukan pada data hasil transformasi agar:

- data RAW tetap dapat direproduksi;
- data downstream lebih mudah dibaca;
- unit lebih sesuai dengan kebutuhan analisis pertanian;
- pemodelan hari basah menggunakan satuan yang umum digunakan.

Unit hasil transformasi adalah:

| Variabel | Unit ERA5-Land | Unit hasil transformasi |
|---|---|---|
| `total_precipitation` | m | mm/day |
| `2m_temperature` | K | °C |
| `2m_dewpoint_temperature` | K | °C |
| `volumetric_soil_water_layer_1` | m³/m³ | tetap m³/m³ |
| `surface_solar_radiation_downwards` | J/m² | tetap J/m²/day |

---

## 1. `total_precipitation`

### Unit sumber

```text
meter (m)
```

ERA5-Land menyimpan total precipitation sebagai kedalaman air dalam meter.

Contoh:

```text
0.047810 m
```

Nilai tersebut kurang intuitif untuk analisis curah hujan.

Dalam meteorologi dan pertanian, curah hujan umumnya dinyatakan dalam:

```text
millimeter (mm)
```

Karena:

```text
1 m = 1000 mm
```

maka:

```text
precipitation_mm = precipitation_m × 1000
```

Contoh:

```text
0.047810 m
× 1000
=
47.810 mm
```

Setelah agregasi harian, unit yang digunakan adalah:

```text
mm/day
```

### Alasan

Penggunaan `mm/day` lebih sesuai karena:

- umum digunakan dalam pengamatan hujan;
- mudah dipahami;
- mudah dibandingkan dengan data stasiun;
- cocok untuk menentukan threshold hari basah;
- lebih praktis untuk analisis pertanian.

---

## 2. `2m_temperature`

### Unit sumber

```text
Kelvin (K)
```

ERA5-Land menyimpan temperatur dalam Kelvin.

Contoh:

```text
294.532 K
```

Untuk penggunaan sehari-hari dan analisis pertanian, Celsius lebih mudah dipahami.

Konversinya:

```text
°C = K - 273.15
```

Contoh:

```text
294.532 - 273.15
=
21.382 °C
```

### Alasan

Celsius dipilih karena:

- umum digunakan di Indonesia;
- mudah diinterpretasikan dalam konteks tanaman;
- lebih mudah digunakan pada visualisasi dan laporan;
- lebih intuitif dibanding Kelvin untuk pengguna non-teknis.

---

## 3. `2m_dewpoint_temperature`

### Unit sumber

```text
Kelvin (K)
```

Sama seperti temperatur udara, dew point temperature juga dikonversi menjadi Celsius:

```text
°C = K - 273.15
```

Contoh:

```text
295 K
- 273.15
=
21.85 °C
```

### Alasan

Dew point berkaitan dengan kelembaban atmosfer.

Menggunakan Celsius memudahkan perbandingan langsung antara:

```text
2m_temperature
```

dan:

```text
2m_dewpoint_temperature
```

Semakin dekat kedua nilai tersebut, semakin tinggi kelembaban relatif udara secara umum.

---

## 4. `volumetric_soil_water_layer_1`

### Unit sumber

```text
m³/m³
```

Variabel ini menunjukkan volume air per volume tanah.

Contoh:

```text
0.30 m³/m³
```

secara sederhana berarti sekitar:

```text
30% volume tanah berupa air
```

jika dibaca sebagai fraksi volumetrik.

Unit ini **tidak dikonversi**.

### Alasan

`m³/m³` sudah merupakan unit standar dan sangat sesuai untuk:

- analisis soil moisture;
- perbandingan kondisi tanah antarhari;
- melihat respons tanah setelah hujan;
- analisis kekeringan atau kejenuhan relatif.

Konversi tambahan tidak diperlukan pada tahap ini.

---

## 5. `surface_solar_radiation_downwards`

### Unit sumber

```text
J/m²
```

Variabel ini merupakan energi radiasi matahari yang mencapai permukaan.

Setelah agregasi harian, interpretasinya menjadi:

```text
J/m²/day
```

Unit ini untuk saat ini **dipertahankan**.

### Alasan

Tidak ada kebutuhan langsung untuk mengubahnya ke unit lain pada tahap awal pemodelan hari basah.

Mempertahankan `J/m²/day` memiliki beberapa keuntungan:

- tetap dekat dengan unit sumber ERA5-Land;
- menghindari transformasi tambahan yang belum diperlukan;
- menjaga pipeline sederhana;
- mudah dikonversi kemudian jika diperlukan.

Sebagai contoh, bila nanti diperlukan untuk analisis agronomi atau energi, unit ini dapat diubah menjadi:

```text
MJ/m²/day
```

dengan:

```text
1 MJ = 1,000,000 J
```

Namun konversi tersebut belum diperlukan untuk baseline model.

---

# Urutan transformasi

Secara konseptual, proses yang dilakukan adalah:

```text
ERA5-Land hourly RAW
        │
        ▼
Validasi waktu UTC
        │
        ▼
Agregasi berdasarkan hari UTC+7
        │
        ▼
valid_time + valid_time_local
        │
        ▼
Konversi unit
        │
        ▼
Parquet + PostgreSQL
```

Konversi unit dilakukan **setelah agregasi harian**.

Contoh precipitation:

```text
RAW accumulated precipitation
        ↓
daily accumulation
        ↓
nilai harian dalam meter
        ↓
× 1000
        ↓
mm/day
```

Contoh temperature:

```text
hourly temperature K
        ↓
daily mean
        ↓
daily mean K
        ↓
- 273.15
        ↓
daily mean °C
```

---

# Unit akhir dataset harian

Setelah P12 dan P14, representasi dataset harian adalah:

```text
valid_time
valid_time_local
latitude
longitude
total_precipitation               mm/day
2m_temperature                    °C
volumetric_soil_water_layer_1     m³/m³
surface_solar_radiation_downwards J/m²/day
2m_dewpoint_temperature           °C
```

---

# Hubungan dengan pemodelan hari basah

Dua tahap ini menjadi fondasi sebelum pembentukan fitur hari basah.

P12 memastikan:

```text
hari yang dianalisis benar secara temporal
```

sedangkan P14 memastikan:

```text
nilai variabel mempunyai unit yang mudah dan tepat untuk dianalisis
```

Khusus curah hujan, penggunaan:

```text
mm/day
```

akan sangat penting pada tahap berikutnya karena definisi hari basah biasanya dibuat menggunakan threshold curah hujan harian.

Dengan demikian pipeline sebelum modelling menjadi:

```text
RAW ERA5-Land hourly
        ↓
temporal validation
        ↓
UTC+7 daily aggregation
        ↓
unit conversion
        ↓
daily climate dataset
        ↓
wet-day feature engineering
        ↓
clustering / seasonal modelling
```

---

# Ringkasan

## P12 — Validasi Temporal

Memastikan bahwa:

- RAW memiliki timestep hourly yang kontinu;
- tidak ada timestamp duplikat;
- hubungan UTC dan UTC+7 benar;
- `valid_time` dan `valid_time_local` berbeda tepat 7 jam;
- setiap hari lokal tersedia;
- tidak terdapat duplikasi record per hari dan grid.

## P14 — Konversi Unit

```text
total_precipitation
m → mm/day

2m_temperature
K → °C

2m_dewpoint_temperature
K → °C

volumetric_soil_water_layer_1
m³/m³ → tetap m³/m³

surface_solar_radiation_downwards
J/m² → tetap J/m²/day
```

Tujuan akhirnya adalah menghasilkan dataset harian yang secara waktu benar, secara unit jelas, dan siap digunakan untuk analisis serta pemodelan hari basah.
