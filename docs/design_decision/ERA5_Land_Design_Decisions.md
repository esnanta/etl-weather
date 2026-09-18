# Keputusan Desain Pipeline ERA5-Land

## 1. Tujuan

Dokumen ini menjelaskan keputusan desain utama pada pipeline pengolahan ERA5-Land. Fokusnya adalah menjaga ketepatan ilmiah data, keterlacakan hasil pengolahan, konsistensi waktu dan ruang, serta keandalan proses ketika data diproses ulang.

Pipeline dibangun untuk menghasilkan data iklim harian yang dapat digunakan sebagai dasar analisis pertanian dan pemodelan pola hari basah di Aceh. Karena itu, setiap tahap pengolahan harus mempertahankan hubungan yang jelas antara data sumber, proses transformasi, hasil harian, dan data yang akhirnya disimpan di PostgreSQL.

---

## 2. Sumber Data

Sumber utama adalah ERA5-Land dari Copernicus Climate Data Store.

Karakteristik yang digunakan dalam pipeline:

- data sumber beresolusi waktu per jam;
- grid reguler latitude-longitude dengan resolusi 0,1°;
- waktu sumber dinyatakan dalam UTC;
- data mentah disimpan dalam bentuk NetCDF;
- hasil transformasi harian disimpan dalam Parquet sebelum dimuat ke PostgreSQL.

Variabel yang saat ini digunakan adalah:

| Kode | Variabel | Statistik harian |
|---|---|---|
| `tp` | total precipitation | total harian |
| `t2m` | 2 metre temperature | rata-rata harian |
| `d2m` | 2 metre dewpoint temperature | rata-rata harian |
| `swvl1` | volumetric soil water layer 1 | rata-rata harian |
| `ssrd` | surface solar radiation downwards | total harian |

Kode asli ERA5-Land dipertahankan selama proses ekstraksi dan transformasi agar hubungan dengan dokumentasi dan sumber data tetap jelas.

---

## 3. Representasi Waktu

Pipeline menyimpan dua representasi waktu:

```text
valid_time
valid_time_local
```

`valid_time` adalah waktu referensi ERA5-Land dalam UTC.

`valid_time_local` adalah representasi waktu lokal UTC+7 yang digunakan untuk analisis berdasarkan hari kalender di Aceh.

Hubungannya adalah:

```text
valid_time_local = valid_time + 7 jam
```

Contoh:

```text
2021-12-31 17:00 UTC
=
2022-01-01 00:00 UTC+7
```

Kedua nilai tersebut menunjukkan momen yang sama.

Penyimpanan kedua kolom waktu diperlukan karena:

- waktu UTC menjaga keterlacakan terhadap sumber ERA5-Land;
- waktu lokal diperlukan agar agregasi harian mengikuti hari yang benar-benar dialami di wilayah penelitian.

Untuk itu, hari lokal dimulai pada pukul 17:00 UTC pada hari sebelumnya dan berakhir tepat sebelum pukul 17:00 UTC pada hari berikutnya.

---

## 4. Pembentukan Data Harian

Data ERA5-Land per jam tidak langsung dianggap sebagai data harian. Setiap variabel diperlakukan sesuai sifat fisiknya.

### 4.1 Variabel keadaan

Temperatur udara, temperatur titik embun, dan kelembaban volumetrik tanah merupakan variabel yang menggambarkan keadaan pada suatu waktu.

Variabel berikut dihitung sebagai rata-rata harian:

```text
t2m
d2m
swvl1
```

Rata-rata dihitung atas seluruh pengamatan per jam yang termasuk dalam satu hari lokal UTC+7.

### 4.2 Variabel akumulasi

Curah hujan dan radiasi matahari ke permukaan merupakan variabel akumulasi:

```text
tp
ssrd
```

Nilai ERA5-Land untuk variabel ini tidak diperlakukan sebagai nilai per jam yang dapat dijumlahkan secara langsung.

Perhitungan harian mengikuti struktur akumulasi ERA5-Land:

```text
Daily(D) =
    A(D 17 UTC)
  + A(D 00 UTC)
  - A(D-1 17 UTC)
```

Pendekatan ini diperlukan agar jumlah harian sesuai dengan batas hari lokal UTC+7.

Akibatnya, permintaan data harus menyertakan konteks waktu sebelum awal periode target. Jika periode analisis dimulai pada 1 Januari, sebagian data dari 31 Desember tetap diperlukan karena awal 1 Januari waktu lokal berada pada 31 Desember pukul 17:00 UTC.

Data tambahan tersebut hanya berfungsi sebagai konteks perhitungan dan tidak menjadi hari analisis tambahan.

---

## 5. Konversi Satuan

Data mentah dipertahankan dalam satuan asli ERA5-Land. Konversi dilakukan setelah agregasi harian agar sumber tetap dapat direproduksi dan hasil analisis lebih mudah diinterpretasikan.

| Variabel | Satuan sumber | Satuan hasil harian |
|---|---|---|
| `tp` | m | mm/day |
| `t2m` | K | °C |
| `d2m` | K | °C |
| `swvl1` | m³/m³ | m³/m³ |
| `ssrd` | J/m² | J/m²/day |

Konversi utama:

```text
precipitation_mm = precipitation_m × 1000
```

dan:

```text
temperature_C = temperature_K - 273.15
```

Kelembaban tanah volumetrik tidak diubah karena satuan m³/m³ sudah sesuai untuk interpretasi ilmiah.

Radiasi matahari dipertahankan dalam J/m²/day. Konversi ke MJ/m²/day dapat dilakukan kemudian jika diperlukan dalam analisis agronomi.

---

## 6. Validasi Temporal

Validasi temporal dilakukan baik pada data mentah maupun hasil harian.

### 6.1 Data mentah

Data per jam harus memenuhi kondisi berikut:

- waktu tersedia dan dapat dibaca sebagai datetime;
- timestamp tersusun secara kronologis;
- tidak terdapat timestamp duplikat;
- interval antarwaktu adalah satu jam;
- tidak terdapat jam yang hilang pada periode yang seharusnya kontinu.

Kelengkapan waktu penting karena data per jam menjadi dasar seluruh perhitungan harian.

### 6.2 Data harian

Hasil harian harus mempertahankan hubungan:

```text
valid_time_local = valid_time + 7 jam
```

Untuk awal hari lokal:

```text
valid_time       = 17:00 UTC
valid_time_local = 00:00 UTC+7
```

Setiap kombinasi berikut hanya boleh muncul satu kali:

```text
valid_time_local
latitude
longitude
```

Selain itu, semua hari lokal yang diminta harus tersedia secara lengkap.

---

## 7. Validasi Spasial

ERA5-Land yang digunakan pada pipeline berada pada grid reguler 0,1°.

Validasi spasial memastikan bahwa:

- latitude dan longitude berada dalam bounding box yang diminta;
- jarak antarkoordinat sesuai resolusi 0,1°;
- jumlah titik grid sesuai dengan rentang koordinat;
- grid valid yang digunakan pada hasil harian konsisten antarhari.

Pada area uji yang terdokumentasi:

```text
latitude  : 3.4 sampai 5.2
longitude : 96.1 sampai 97.6
```

diperoleh:

```text
19 latitude
16 longitude
304 titik grid geometris
```

Tidak seluruh titik geometris menjadi data harian valid. Sebagian titik dapat berada di laut atau tidak memiliki nilai permukaan daratan yang valid.

Pada hasil uji, terdapat:

```text
249 grid daratan valid
```

untuk setiap hari.

Yang diperiksa bukan hanya jumlah grid, tetapi juga pasangan latitude-longitude yang digunakan. Set grid valid harus tetap sama dari hari ke hari.

---

## 8. Nilai Hilang

Nilai hilang tidak boleh dianggap sebagai nol.

```text
NaN -> NULL
```

Perbedaan ini penting terutama pada curah hujan.

```text
tp = 0
```

berarti tidak ada hujan yang tercatat.

Sedangkan:

```text
tp = NULL
```

berarti nilai tidak tersedia.

Menyamakan keduanya dapat menimbulkan bias pada analisis hari basah, frekuensi hujan, maupun pemodelan berikutnya.

---

## 9. Penyimpanan Data Mentah dan Hasil Transformasi

Pipeline menyimpan dua artefak utama:

```text
NetCDF  -> data mentah
Parquet -> data harian hasil transformasi
```

Data mentah diperlakukan sebagai sumber ilmiah yang dipertahankan setelah berhasil diunduh.

Data mentah tidak ditimpa secara diam-diam. Jika artefak untuk permintaan yang sama sudah tersedia, pipeline menggunakan kembali file tersebut setelah validasi.

Parquet menyimpan hasil transformasi harian dan juga dapat digunakan kembali jika masih konsisten dengan data mentah yang menjadi sumbernya.

Prinsip yang digunakan adalah:

```text
file tersedia != file otomatis valid
```

Karena itu, penggunaan ulang artefak selalu diikuti pemeriksaan integritas dan validasi data.

---

## 10. Identitas Permintaan Data

Setiap permintaan data memiliki identitas deterministik.

Identitas tersebut dibentuk dari unsur yang menentukan isi data, antara lain:

```text
sumber data
dataset
periode
himpunan variabel
bounding box
```

Urutan variabel tidak mengubah makna ilmiah permintaan. Permintaan dengan himpunan variabel yang sama harus menghasilkan identitas yang sama walaupun urutan penulisannya berbeda.

Identitas deterministik diperlukan agar pipeline dapat mengenali bahwa suatu permintaan pernah diproses sebelumnya.

---

## 11. Keterlacakan Data

Setiap hasil transformasi harus dapat ditelusuri kembali ke data mentah yang digunakan.

Data mentah menyimpan waktu ketika proses pengunduhan selesai:

```text
downloaded_at
```

Checksum SHA-256 dihitung dari isi file untuk mendeteksi perubahan pada artefak.

Metadata Parquet menyimpan:

```text
raw_artifact_path
raw_sha256
downloaded_at
transformed_at
```

PostgreSQL menyimpan informasi artefak transformasi:

```text
downloaded_at
transformed_artifact_path
transformed_sha256
transformed_at
```

Dengan demikian, rantai keterlacakan adalah:

```text
PostgreSQL
    |
    v
Parquet
    |
    v
NetCDF mentah
```

Jika checksum data mentah berbeda dari checksum yang tercatat pada Parquet, hasil transformasi lama tidak boleh digunakan kembali.

Pendekatan ini mencegah penggunaan hasil transformasi yang sudah tidak sesuai dengan sumbernya.

---

## 12. Penyimpanan di PostgreSQL

Data harian yang telah lolos validasi disimpan dalam schema:

```text
climate_data
```

dengan dua tabel utama:

```text
daily_climate
daily_climate_detail
```

### 12.1 Metadata dataset

`daily_climate` menyimpan identitas dan metadata satu partisi data, antara lain:

```text
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

### 12.2 Observasi harian

`daily_climate_detail` menyimpan observasi untuk setiap grid dan hari lokal.

Kolom utama:

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

Identitas satu observasi adalah:

```text
daily_climate_id
+ valid_time
+ latitude
+ longitude
```

Jika data yang sama diproses kembali, baris yang sama diperbarui dan tidak dibuat duplikat baru.

Dengan demikian, pengulangan proses tidak mengubah jumlah observasi secara tidak semestinya.

---

## 13. Akses Data untuk Analisis

Setelah data melalui ekstraksi, transformasi, validasi, dan penyimpanan, PostgreSQL menjadi sumber utama untuk pengambilan data harian bagi analisis berikutnya.

Akses dilakukan melalui lapisan pembacaan yang bersifat hanya-baca.

Pemilihan data dilakukan secara eksplisit berdasarkan:

```text
source
dataset_name
periode
bounding box
```

Hasil pembacaan berupa DataFrame yang telah diperiksa terhadap:

- kelengkapan kolom;
- periode yang diminta;
- waktu lokal tengah malam;
- duplikasi grid per hari;
- hasil kosong;
- urutan data yang deterministik.

Lapisan ini tidak melakukan imputasi, normalisasi, klasifikasi hari basah, maupun pemilihan fitur model.

Dengan pemisahan ini, data iklim yang telah divalidasi tidak berubah hanya karena metode analisis atau model berubah.

---

## 14. Partisi Bulanan

Untuk pengolahan historis jangka panjang, data dibagi menjadi partisi bulanan.

Setiap bulan memiliki:

```text
identitas permintaan
NetCDF mentah
Parquet hasil transformasi
metadata PostgreSQL
```

Pendekatan bulanan membatasi dampak kegagalan. Jika satu bulan gagal diproses, bulan lain yang sudah selesai tidak perlu dihitung ulang.

Ini juga membuat pengolahan historis lebih mudah dilanjutkan, diperiksa, dan diulang secara selektif.

---

## 15. Pemisahan Pengolahan Ilmiah dan Orkestrasi

Seluruh logika yang menentukan makna ilmiah data tetap berada di dalam pipeline.

Termasuk di dalamnya:

- pemilihan data sumber;
- interpretasi waktu UTC dan UTC+7;
- agregasi harian;
- perlakuan variabel akumulasi;
- konversi satuan;
- validasi temporal;
- validasi spasial;
- pemeriksaan nilai hilang;
- keterlacakan artefak;
- penyimpanan data.

Sistem orkestrasi hanya bertugas mengatur kapan suatu proses dijalankan, ketergantungan antarproses, pengulangan ketika gagal, dan pembagian periode kerja.

Pemisahan ini menjaga agar perubahan pada mekanisme penjadwalan tidak mengubah hasil ilmiah data.

---

## 16. Prinsip Desain yang Harus Dipertahankan

Keputusan berikut dianggap sebagai bagian dari kontrak ilmiah dan arsitektur data:

1. data per jam ERA5-Land tetap disimpan sebagai data mentah;
2. waktu UTC dan waktu lokal UTC+7 sama-sama dipertahankan;
3. pembentukan hari mengikuti kalender lokal;
4. setiap variabel diagregasi sesuai sifat fisiknya;
5. variabel akumulasi ERA5-Land tidak dijumlahkan seperti data hourly biasa;
6. data mentah tetap menggunakan satuan sumber;
7. konversi satuan dilakukan pada hasil transformasi;
8. nilai hilang tidak disamakan dengan nol;
9. grid hasil harian harus stabil antarhari;
10. data mentah tidak ditimpa secara diam-diam;
11. penggunaan ulang artefak tetap memerlukan validasi;
12. hasil transformasi harus dapat ditelusuri ke data mentah;
13. pemrosesan ulang tidak boleh menghasilkan duplikasi di database;
14. data iklim tersimpan dipisahkan dari logika pemodelan;
15. definisi hari basah dan pemilihan fitur model dilakukan setelah data historis yang memadai tersedia.

---

## 17. Hasil Verifikasi

Pengujian terakhir yang terdokumentasi menghasilkan:

```text
61 passed
```

Pada dataset uji tiga hari:

```text
3 hari lokal
249 grid daratan valid per hari
747 observasi
```

Hubungan tersebut konsisten:

```text
249 × 3 = 747
```

Pengujian juga menunjukkan bahwa ketika permintaan yang sama dijalankan kembali:

- data mentah digunakan kembali;
- hasil transformasi digunakan kembali setelah pemeriksaan;
- identitas dataset tetap sama;
- observasi tidak bertambah menjadi duplikat;
- keterlacakan artefak tetap konsisten.

---

## 18. Kesimpulan

Pipeline ERA5-Land dirancang dengan prinsip utama bahwa ketepatan ilmiah data harus tetap terjaga dari sumber sampai data digunakan untuk analisis.

Alur utamanya adalah:

```text
ERA5-Land hourly
        |
        v
NetCDF mentah
        |
        v
validasi
        |
        v
agregasi hari lokal UTC+7
        |
        v
konversi satuan
        |
        v
validasi hasil harian
        |
        v
Parquet
        |
        v
PostgreSQL
        |
        v
data harian terkontrol untuk analisis
```

Desain ini mempertahankan tiga hal utama:

- ketepatan waktu, ruang, dan satuan;
- keterlacakan hasil terhadap data sumber;
- kemampuan memproses ulang data tanpa menghasilkan inkonsistensi atau duplikasi.

Dengan dasar tersebut, data historis dapat dikembangkan lebih lanjut untuk analisis hari basah, pola musim, dan pemodelan tanpa mengubah kontrak ilmiah pada lapisan pengolahan data.
