EDA Tahap 1 — Raw Data Assessment
input:
analytics/era5_land_2016_2025.parquet

Tujuan:
1. Mengetahui kelengkapan data hourly.
2. Mengidentifikasi NULL, NaN, dan missing observations.
3. Mengidentifikasi anomali atau nilai tidak wajar.
4. Memahami distribusi masing-masing variabel.
5. Memeriksa variasi temporal hourly.
6. Memeriksa distribusi spasial.

Output:
eda/
└── raw_assessment/
    ├── reports/
    └── figures/


EDA Tahap 2 — Daily Climate Analysis
input:
transformed/
└── YYYY/
    └── *.parquet

Tujuan:
1. Menganalisis distribusi curah hujan harian.
2. Mengidentifikasi karakteristik hari basah.
3. Mengkaji pola musiman dan antar-tahun.
4. Menganalisis hubungan antarfaktor meteorologi.
5. Menentukan variabel yang relevan untuk clustering.
6. Mengidentifikasi kebutuhan feature engineering.
Output:
eda/
└── daily_analysis/
    ├── reports/
    └── figures/


Hasil kedua tahap EDA kemudian menjadi dasar penyusunan dataset fitur.

Keuntungan utamanya: Kita dapat membedakan masalah yang berasal dari data sumber dengan masalah yang muncul setelah transformasi.

Misalnya, jika terdapat nilai curah hujan harian yang tidak wajar, kita dapat menelusuri apakah fenomena tersebut memang muncul pada raw atau berkaitan dengan perhitungan accumulated variables.