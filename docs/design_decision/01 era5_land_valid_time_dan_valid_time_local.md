# Penjelasan `valid_time` dan `valid_time_local` pada ERA5-Land

## Tujuan

Pipeline menyimpan dua representasi waktu:

- `valid_time`: waktu asli ERA5-Land dalam **UTC**
- `valid_time_local`: waktu lokal Aceh/Indonesia bagian barat dalam **UTC+7**

Keduanya dipertahankan agar data tetap dapat dilacak ke waktu sumber ERA5-Land, sekaligus mudah digunakan untuk analisis berdasarkan hari lokal.

## Mengapa 1 Januari 2022 pukul 00:00 waktu lokal adalah 31 Desember 2021 pukul 17:00 UTC?

Aceh menggunakan zona waktu **UTC+7**. Artinya waktu lokal berada **7 jam lebih maju** daripada UTC.

```text
Waktu lokal = UTC + 7 jam
UTC = Waktu lokal - 7 jam
```

Jika waktu lokal adalah:

```text
2022-01-01 00:00:00 UTC+7
```

maka dikurangi 7 jam:

```text
2022-01-01 00:00:00
-                 07:00
--------------------------------
2021-12-31 17:00:00 UTC
```

Jadi:

```text
valid_time       = 2021-12-31 17:00:00
valid_time_local = 2022-01-01 00:00:00
```

Kedua nilai tersebut menunjukkan **momen yang sama**, hanya ditulis dalam zona waktu yang berbeda.

## Ilustrasi sederhana

```text
UTC                              Waktu lokal UTC+7
────────────────────────────────────────────────────
2021-12-31 16:00        →        2021-12-31 23:00
2021-12-31 17:00        →        2022-01-01 00:00
2021-12-31 18:00        →        2022-01-01 01:00
...
2022-01-01 16:00        →        2022-01-01 23:00
2022-01-01 17:00        →        2022-01-02 00:00
```

Karena itu, satu hari penuh **1 Januari 2022 waktu lokal** secara UTC berada pada rentang:

```text
2021-12-31 17:00 UTC
sampai
2022-01-01 16:59:59 UTC
```

Untuk data hourly, timestamp yang digunakan adalah:

```text
2021-12-31 17:00 UTC
...
2022-01-01 16:00 UTC
```

sebanyak 24 timestep.

## Mengapa pipeline menyimpan dua kolom waktu?

### `valid_time`

Kolom ini mempertahankan referensi waktu sumber ERA5-Land dalam UTC.

Keuntungannya:

- tetap konsisten dengan sumber ERA5-Land;
- mudah dibandingkan dengan dataset lain yang menggunakan UTC;
- menjaga provenance dan keterlacakan data;
- tidak kehilangan konteks waktu asli.

### `valid_time_local`

Kolom ini digunakan untuk kebutuhan analisis lokal.

Keuntungannya:

- lebih mudah membaca hari kalender lokal;
- cocok untuk analisis pertanian;
- cocok untuk pembentukan hari basah;
- mempermudah agregasi harian dan analisis pola musim.

## Contoh dari hasil pipeline

```text
valid_time               valid_time_local
2021-12-31 17:00:00      2022-01-01 00:00:00
2022-01-01 17:00:00      2022-01-02 00:00:00
2022-01-02 17:00:00      2022-01-03 00:00:00
```

Ini bukan pergeseran atau kesalahan tanggal. Setiap pasangan menunjukkan awal hari lokal yang sama:

```text
31 Des 2021 17:00 UTC = 1 Jan 2022 00:00 UTC+7
1 Jan 2022 17:00 UTC  = 2 Jan 2022 00:00 UTC+7
2 Jan 2022 17:00 UTC  = 3 Jan 2022 00:00 UTC+7
```

## Hubungan dengan agregasi harian ERA5-Land

ERA5-Land menggunakan UTC sebagai referensi waktu.

Karena tujuan penelitian adalah membentuk data historis untuk membaca pola hari basah dalam konteks lokal, pembentukan data harian harus mengikuti batas hari lokal UTC+7, bukan batas hari UTC.

Dengan demikian:

```text
Hari lokal 1 Januari 2022
```

dimulai pada:

```text
31 Desember 2021 pukul 17:00 UTC
```

dan berakhir tepat sebelum:

```text
1 Januari 2022 pukul 17:00 UTC
```

Pendekatan ini memastikan curah hujan, suhu, kelembaban tanah, radiasi matahari, dan variabel lain dikelompokkan sesuai hari yang benar-benar dialami di wilayah Aceh.

## Ringkasan

```text
ERA5-Land source time     : UTC
Zona waktu lokal          : UTC+7
Selisih                   : +7 jam

2021-12-31 17:00 UTC
        =
2022-01-01 00:00 UTC+7
```

Karena itu pipeline mempertahankan `valid_time` sebagai waktu UTC asli dan menambahkan `valid_time_local` sebagai representasi waktu lokal UTC+7.

Pemisahan ini membuat data tetap akurat secara temporal dan lebih mudah digunakan untuk analisis hari basah serta pola musim pertanian.
