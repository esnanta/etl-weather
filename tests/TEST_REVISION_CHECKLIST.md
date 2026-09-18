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

## Langkah selanjutnya — prioritas tinggi

- [ ] Tambahkan integration test PostgreSQL terisolasi untuk membuktikan pemuatan ulang tidak menghasilkan duplikat.
- [ ] Verifikasi transaksi PostgreSQL ketika pemuatan detail gagal di tengah proses.

## Langkah selanjutnya — penguatan validasi

- [ ] Tentukan rentang fisik yang sah untuk setiap variabel sebelum menambahkan range-validation test.
- [ ] Tambahkan test lebih dari satu grid untuk rumus akumulasi dan mean harian.
- [ ] Tambahkan test stabilitas grid ketika nilai hilang muncul hanya pada salah satu hari.
- [ ] Tambahkan test kesalahan metadata provenance Parquet selain perubahan checksum.
- [ ] Tambahkan test kegagalan penulisan atomik tanpa meninggalkan artefak akhir parsial.
- [ ] Audit dependensi yang hanya diperlukan jalur ARCO dan hapus jika tidak lagi digunakan.

## Sinkronisasi dokumentasi setelah test stabil

- [ ] Perbarui `tests/AGENTS.md` dan `tests/README_TESTING.md`; hapus status lama, angka baseline lama, dan teks literal `\n`.
- [ ] Perbarui README proyek dan panduan repository agar hanya menyatakan CDS ERA5-Land sebagai sumber aktif.
- [ ] Sinkronkan dokumen keputusan desain setelah test prioritas tinggi lulus.
- [ ] Tempatkan penjelasan test pada bagian ilmiah yang relevan, bukan pada satu bab testing terpisah.
