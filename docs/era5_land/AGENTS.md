# Pedoman Penyusunan Dokumen Keputusan Desain

## 1. Pembaca utama

Dokumen harus dapat dipahami oleh tiga kelompok:

- pemangku kepentingan non-IT;
- pembaca berlatar meteorologi atau klimatologi; dan
- developer yang perlu mengetahui konsekuensi implementasinya.

Karena itu, penjelasan dimulai dari makna ilmiah dan dampaknya terhadap data. Nama fungsi, nama modul, formula teknis, dan detail database digunakan hanya ketika diperlukan.

Contohnya, jangan langsung menulis “pipeline melakukan UPSERT berdasarkan `request_key`”. Penjelasan utamanya menjadi:

> Menjalankan kembali permintaan data yang sama tidak boleh membuat observasi ganda. Sistem mengenali permintaan yang sama berdasarkan sumber, periode, variabel, dan wilayah. Secara teknis, identitas tersebut disimpan sebagai `request_key`.

## 2. ERA5-Land sebagai satu-satunya fokus

Dokumen akan membahas CDS ERA5-Land sebagai sumber data aktif. Pembahasan ARCO dan anggapan bahwa repository masih memiliki beberapa jalur sumber akan dihapus.

Konsekuensinya:

- judul dan ruang lingkup menegaskan CDS ERA5-Land;
- contoh seluruhnya menggunakan ERA5-Land;
- istilah “dataset lain” atau “jalur ARCO” dihilangkan; dan
- keputusan tidak lagi dijelaskan sebagai percabangan antarpenyedia.

## 3. Bentuk baku setiap keputusan

Setiap keputusan utama akan mengikuti pola yang konsisten:

- **Keputusan** — apa yang dilakukan sistem.
- **Mengapa keputusan ini diperlukan** — persoalan ilmiah atau operasional yang dihindari.
- **Dasar ilmiah** — hubungan dengan karakteristik ERA5-Land dan dokumentasi resmi.
- **Cara penerapan** — gambaran implementasi dalam bahasa sederhana.
- **Contoh** — contoh waktu, nilai, koordinat, atau struktur data.
- **Bagaimana keputusan diuji** — perilaku yang diperiksa oleh test.
- **Batasan** — hal yang belum dijamin oleh sistem.

Dengan struktur tersebut, keputusan tidak hanya menjelaskan “apa”, tetapi juga “mengapa” dan “bagaimana kita mengetahui penerapannya benar”.

## 4. Dasar ilmiah dan dasar rekayasa dipisahkan

Alasan sebuah keputusan harus diklasifikasikan dengan jelas:

- **Dasar ilmiah** berasal dari sifat ERA5-Land, misalnya arti parameter akumulasi, satuan, timestamp, resolusi grid, dan batas hari.
- **Dasar kebutuhan pengguna** berasal dari konteks analisis, misalnya penggunaan hari kalender UTC+7.
- **Dasar keandalan sistem** mencakup penulisan atomik, checksum, resume, transaksi, dan pencegahan duplikasi.
- **Batas implementasi** menjelaskan pilihan yang belum merupakan kaidah ilmiah, misalnya format nama file atau ukuran kelompok pemuatan database.

Pemisahan ini mencegah keputusan teknis seolah-olah dianggap sebagai ketentuan meteorologis.

## 5. Referensi ERA5-Land ditempatkan dekat keputusan

Dokumentasi resmi Copernicus/ECMWF akan menjadi rujukan utama untuk:

- karakteristik ERA5-Land;
- resolusi grid;
- cakupan waktu;
- arti waktu valid;
- perbedaan variabel keadaan dan akumulasi;
- konvensi `tp` dan `ssrd`; serta
- satuan asli setiap variabel.

Referensi tidak hanya dikumpulkan di bagian akhir. Setiap keputusan ilmiah akan memiliki rujukan di bagian yang membahas keputusan tersebut. Daftar referensi ringkas tetap dapat dipertahankan pada akhir dokumen.

Jika suatu keputusan merupakan interpretasi proyek—bukan pernyataan langsung dari dokumentasi ERA5-Land—hal itu akan disebutkan dengan jelas.

## 6. Test ditempatkan pada pembahasan yang relevan

Tidak akan dibuat satu bab panjang yang hanya mendaftar seluruh test. Bukti pengujian ditempatkan setelah keputusan yang diuji.

Contoh penempatannya:

| Pembahasan | Test yang dijelaskan |
| --- | --- |
| Hari lokal UTC+7 | pemeriksaan selisih tujuh jam, pergantian tanggal, dan tengah malam lokal |
| Akumulasi `tp` dan `ssrd` | pemeriksaan rumus tiga nilai batas dan hasil numeriknya |
| Mean harian | pemeriksaan penggunaan 24 jam dan kegagalan ketika satu sampel hilang |
| Beberapa grid | pemeriksaan agar nilai antarkoordinat tidak tercampur |
| Nilai hilang | pemeriksaan bahwa nilai hilang tidak berubah menjadi nol |
| Stabilitas grid | pemeriksaan bahwa nilai hilang tidak otomatis menghapus identitas grid |
| Artefak RAW dan Parquet | pemeriksaan checksum, metadata provenance, dan kegagalan penulisan atomik |
| PostgreSQL | pemeriksaan pemuatan ulang tanpa duplikasi serta rollback ketika pemuatan gagal |
| Akses data analisis | pemeriksaan periode, wilayah, struktur kolom, dan observasi ganda |

Nama file atau fungsi test dapat dicantumkan sebagai catatan teknis singkat, tetapi narasi utama menjelaskan perilaku yang dibuktikan.

## 7. Test bukan pengganti dasar ilmiah

Dokumen akan membedakan dua jenis keyakinan:

- dokumentasi ERA5-Land menjelaskan bagaimana data seharusnya ditafsirkan;
- test membuktikan bahwa program menerapkan interpretasi yang telah dipilih secara konsisten.

Sebagai contoh, test yang menghasilkan total presipitasi 8 mm membuktikan implementasi mengikuti rumus yang ditetapkan. Test tersebut tidak dengan sendirinya membuktikan bahwa rumus itu benar secara ilmiah; dasar ilmiahnya tetap harus berasal dari dokumentasi ECMWF/Copernicus.

Ini penting agar pemangku kepentingan tidak mengartikan “test lulus” sebagai validasi ilmiah menyeluruh terhadap data reanalysis.

## 8. Contoh dibuat berlapis

Setiap konsep yang berpotensi membingungkan akan diberi contoh sederhana terlebih dahulu, kemudian detail teknis bila diperlukan.

Untuk hari lokal UTC+7, urutannya akan seperti ini:

- 2 Januari waktu Aceh dimulai pada 1 Januari pukul 17.00 UTC.
- Karena data ERA5-Land memakai waktu UTC, sistem memerlukan sebagian data dari tanggal sebelumnya.
- Contoh numerik menunjukkan bagaimana nilai batas membentuk total 24 jam.
- Setelah itu baru ditampilkan formula formal dan kaitannya dengan test.

Contoh akan menggunakan tanggal, koordinat, dan angka kecil yang dapat diperiksa secara manual.

## 9. Istilah teknis dikurangi dan langsung dijelaskan

Beberapa penyesuaian bahasa:

| Istilah | Penjelasan |
| --- | --- |
| idempotensi | pemuatan ulang tidak membuat duplikat |
| provenance | catatan asal-usul data |
| atomic write | file akhir hanya muncul jika penulisan selesai |
| rollback transaksi | jika sebagian pemuatan gagal, seluruh perubahan dibatalkan |
| variabel instantaneous | keadaan pada waktu tertentu |
| akumulasi | tetap digunakan karena relevan secara meteorologis, disertai penjelasan cara ERA5-Land mencatatnya |

Istilah teknis yang perlu dipertahankan akan dijelaskan ketika pertama kali digunakan.

## 10. Fakta, keputusan, hasil test, dan keterbatasan tidak dicampur

Saya akan menggunakan penanda naratif yang jelas:

- **Fakta ERA5-Land:** karakteristik dari penyedia.
- **Keputusan proyek:** cara proyek mengolah karakteristik tersebut.
- **Bukti pengujian:** perilaku program yang sudah diperiksa.
- **Keterbatasan:** kondisi yang belum ditangani atau belum dibuktikan.

Ini membantu pembaca mengetahui mana yang berasal dari Copernicus dan mana yang merupakan pilihan proyek.

## 11. Struktur dokumen yang diusulkan

- Tujuan dan pembaca dokumen
- Data yang digunakan: CDS ERA5-Land
- Gambaran perjalanan data
- Pemilihan variabel dan satuan
- Wilayah, grid, dan identitas lokasi
- Permintaan data dan pembagian periode
- Waktu UTC dan hari lokal UTC+7
- Rata-rata harian variabel keadaan
- Total harian presipitasi dan radiasi
- Nilai hilang dan kelengkapan data
- Pemeriksaan kewajaran serta konsistensi hasil
- Penyimpanan RAW dan transformed
- Catatan asal-usul data dan pemeriksaan checksum
- PostgreSQL, pencegahan duplikasi, dan pemulihan kegagalan
- Akses data untuk analisis
- Batasan yang masih berlaku
- Ringkasan keputusan
- Referensi resmi

Penjelasan test akan berada di dalam bagian 4–15 sesuai keputusan yang diperiksanya.

Detail teknis yang tetap berguna bagi developer akan dipertahankan secara ringkas, ditempatkan setelah penjelasan umum atau dalam catatan teknis.

## Kriteria selesai

Revisi dapat dianggap selesai apabila:

- seluruh keputusan sesuai dengan implementasi CDS ERA5-Land saat ini;
- setiap keputusan penting memiliki alasan;
- keputusan ilmiah mempunyai dasar resmi;
- setiap konsep penting memiliki contoh yang dapat diperiksa;
- test dijelaskan pada bagian keputusan yang dibuktikannya;
- test tidak diklaim sebagai pengganti validasi ilmiah;
- ARCO tidak lagi disebut sebagai jalur aktif;
- pembaca non-IT dapat memahami tujuan dan dampak keputusan tanpa harus membaca kode; dan
- nama file, jalur data, satuan, dan batasan sesuai dengan keadaan repository terbaru.