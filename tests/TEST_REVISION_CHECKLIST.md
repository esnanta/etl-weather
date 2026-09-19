# CDS ERA5-Land Test Revision Checklist

Checklist ini melacak perbaikan test untuk jalur aktif Copernicus CDS ERA5-Land.
ARCO dan ERA5 single-levels tidak termasuk dalam ruang lingkup pipeline aktif.

## Selesai dan terverifikasi

- [x] Membaca `tests/AGENTS.md` dan `tests/README_TESTING.md` sebelum mengubah test.
- [x] Menghapus `DataSource.ARCO` dan profil dataset ARCO dari pipeline.
- [x] Menghapus dukungan CDS ERA5 single-levels agar kontrak sumber hanya ERA5-Land.
- [x] Menghapus extractor, runtime branch, entry point, dan integration test ARCO.
- [x] Mengubah konfigurasi dan test konfigurasi menjadi khusus CDS ERA5-Land.
- [x] Memperbaiki `test_extract.py` yang tidak lagi sesuai dengan `WeatherRequest`.
- [x] Menguji payload CDS: variabel, hari, 24 jam UTC, bounding box, dan format NetCDF.
- [x] Menguji konteks satu hari sebelum periode target.
- [x] Menguji agar konteks tidak melewati awal ERA5-Land pada 1 Januari 1950.
- [x] Menguji penolakan payload yang melintasi bulan kalender.
- [x] Menguji pembagian periode panjang menjadi partisi bulanan.
- [x] Menguji stabilitas `request_key` terhadap urutan variabel.
- [x] Menguji perubahan `request_key` ketika periode atau bounding box berubah.
- [x] Menguji waktu UTC dan UTC+7 pada hasil harian.
- [x] Menguji rumus batas akumulasi `tp` dan `ssrd`.
- [x] Menguji mean variabel keadaan dari 24 nilai hourly yang bervariasi.
- [x] Menguji konversi Kelvin ke Celsius dan meter ke milimeter.
- [x] Menguji bahwa `swvl1` tetap dalam m³/m³.
- [x] Menguji bahwa satu sampel keadaan `NaN` menggagalkan hari yang tidak lengkap.
- [x] Menguji bahwa batas akumulasi `NaN` tetap menjadi nilai hilang.
- [x] Menguji bahwa akumulasi negatif dipertahankan dan menghasilkan warning.
- [x] Menguji penghapusan metadata sumber dari observasi hasil transformasi.
- [x] Mengisolasi test API key yang hilang dari `CDSAPI_KEY` pada berkas `.env` pengembang.
- [x] Menjalankan seluruh unit test setelah penghapusan ARCO: `73 passed`.
- [x] Menambahkan bounding box ke identitas jalur artefak RAW dan Parquet.
- [x] Menguji bahwa dua bounding box berbeda tidak menghasilkan jalur artefak yang sama.
- [x] Menjalankan seluruh unit test setelah identitas bounding box ditambahkan: `75 passed`.
- [x] Mengubah root artefak menjadi `cds_reanalysis-era5-land` dan menghapus direktori sumber yang redundan di bawah layer.
- [x] Menambahkan test `CdsExtractor.extract()` dengan klien CDS palsu untuk konteks waktu, normalisasi, dan pemilihan variabel tanpa jaringan.
- [x] Menjalankan seluruh unit test setelah test extractor terpadu ditambahkan: `76 passed`.
- [x] Menguji kegagalan eksplisit transformasi hari lokal 1 Januari 1950 karena konteks UTC+7 dimulai sebelum cakupan ERA5-Land.
- [x] Menjalankan seluruh unit test setelah proteksi hari pertama ERA5-Land ditambahkan: `77 passed`.
- [x] Menambahkan integration test PostgreSQL dengan database test dan schema unik untuk membuktikan idempotensi pemuatan ulang.
- [x] Memuat konfigurasi integration test PostgreSQL dari `.env` melalui variabel `TEST_POSTGRES_*`.
- [x] Menjalankan integration test dengan PostgreSQL test lokal: `1 passed`.
- [x] Menambahkan integration test kegagalan detail untuk membuktikan rollback metadata dan observasi dalam satu transaksi.
- [x] Menjalankan seluruh integration test PostgreSQL termasuk rollback: `2 passed`.
- [x] Menetapkan batas fisik keras transformed data: suhu tidak di bawah nol absolut, `swvl1` dalam 0–1, dan semua nilai non-missing harus finite.
- [x] Menambahkan range-validation test tanpa menolak missing value atau akumulasi negatif yang memang dipertahankan dengan warning.
- [x] Menjalankan seluruh unit test setelah range validation ditambahkan: `82 passed`.
- [x] Menguji rumus akumulasi dan mean harian pada lebih dari satu grid tanpa pencampuran antar-koordinat.
- [x] Menjalankan seluruh unit test setelah pengujian multi-grid ditambahkan: `83 passed`.
- [x] Menguji bahwa satu nilai missing pada satu hari tidak mengubah keanggotaan grid selama baris koordinat tetap ada.
- [x] Menjalankan seluruh unit test setelah pengujian missing value per hari ditambahkan: `84 passed`.
- [x] Menguji penolakan metadata provenance Parquet yang hilang, path RAW yang berbeda, dan timestamp yang rusak.
- [x] Menjalankan seluruh unit test setelah penguatan provenance Parquet: `88 passed`.
- [x] Menguji kegagalan penulisan atomik NetCDF dan Parquet tanpa meninggalkan file final atau sementara parsial.
- [x] Menjalankan seluruh unit test setelah pengujian atomic-write failure: `90 passed`.
- [x] Mengaudit import aktif dan dependency graph untuk memisahkan dependensi CDS ERA5-Land dari dependensi jalur ARCO.
- [x] Menghapus 35 dependensi langsung dan transitif jalur ARCO, termasuk stack Zarr, Google Cloud Storage, async HTTP, dan Dask.
- [x] Memvalidasi instalasi dari virtual environment baru: `pip check` bersih dan dependensi ARCO tidak terpasang.
- [x] Menjalankan seluruh unit test dari virtual environment baru setelah pembersihan dependensi: `90 passed`.
- [x] Mengaudit warning ABI `numpy`–`netCDF4`; mempertahankan versi, membiarkan warning tetap terlihat, dan tidak menambahkan filter global.
- [x] Memperbarui `tests/AGENTS.md` dan `tests/README_TESTING.md`; menghapus status lama, baseline lama, dan separator baris yang terescape.
- [x] Mengganti kode tahap internal dengan istilah teknis yang langsung menjelaskan kontrak, risiko, atau perilaku yang diuji.

## Langkah selanjutnya — penguatan validasi

Tidak ada item penguatan validasi yang masih terbuka pada checklist saat ini.

## Sinkronisasi dokumentasi setelah test stabil

- [ ] Perbarui README proyek dan panduan repository agar hanya menyatakan CDS ERA5-Land sebagai sumber aktif.
- [ ] Sinkronkan dokumen keputusan desain setelah test prioritas tinggi lulus.
- [ ] Tempatkan penjelasan test pada bagian ilmiah yang relevan, bukan pada satu bab testing terpisah.
