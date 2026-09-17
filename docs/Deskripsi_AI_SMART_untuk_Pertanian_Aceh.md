# AI SMART untuk Pertanian Aceh: Integrasi Keuneunong dan AI Cuaca

## Deskripsi Inovasi Aplikasi

Aplikasi ini adalah platform Android yang memadukan kearifan lokal Aceh, khususnya fase Keuneunong, dengan Artificial Intelligence (AI) dan data meteorologi modern. Tujuannya adalah membaca dinamika cuaca secara historis tanpa melepaskan konteks budaya, sehingga pengguna dapat memahami hubungan pengetahuan lokal dan perubahan iklim secara lebih mudah.

Fokus pengembangan aplikasi ini adalah melestarikan sekaligus meningkatkan pemahaman masyarakat terhadap pengetahuan cuaca lokal, serta membantu petani lebih adaptif dalam membaca pola musim. Dengan informasi yang akurat, petani dapat menggunakan aplikasi sebagai acuan untuk menentukan waktu tanam dan pengelolaan lahan yang optimal.

## Cara Kerja dan Pemrosesan Data

Di balik antarmuka Android, sistem backend memproses data cuaca historis dari Copernicus dengan rentang minimal 10 tahun terakhir. Analisis dilakukan menggunakan lima parameter meteorologi utama:

- Curah hujan total (*total precipitation*)
- Suhu permukaan (*2m temperature*)
- Kandungan air tanah (*volumetric soil water layer 1*)
- Radiasi matahari (*surface solar radiation downwards*)
- Suhu titik embun (*2m dewpoint temperature*)

AI menganalisis kelima parameter tersebut untuk memodelkan pengelompokan hari basah. Hasil pemodelan berbasis data (*data-driven*) kemudian disandingkan dengan fase Keuneunong yang sedang berlangsung pada periode yang sama.

## Output dan Nilai Tambah Aplikasi

Berdasarkan perpaduan pemodelan AI historis dan fase Keuneunong, sistem menghasilkan tiga output utama:

1. **Saran dan strategi (rekomendasi tindakan)**  
   AI memberikan panduan praktis berdasarkan kondisi saat ini dan data historis. Contohnya, saat *volumetric soil water* optimal dan fase Keuneunong tertentu aktif, aplikasi dapat merekomendasikan jenis tanaman yang cocok atau strategi irigasi yang efisien.
2. **Risiko yang perlu diwaspadai (peringatan dini)**  
   Aplikasi mengidentifikasi potensi ancaman dari pola curah hujan historis dan kelembapan. Pengguna menerima peringatan risiko gagal panen akibat kekeringan (suhu dan radiasi matahari tinggi) atau risiko banjir/hama (prediksi hari basah mendominasi).
3. **Proyeksi skenario (antisipasi masa depan) — fitur opsional**  
   AI menyajikan beberapa kemungkinan skenario cuaca beberapa minggu ke depan, misalnya: Skenario A (hujan ringan berkepanjangan) dan Skenario B (kemarau basah). Fitur ini membantu pengguna menyiapkan rencana cadangan saat cuaca berubah mendadak.

## Saran Pengembangan Aplikasi

- **Penyederhanaan bahasa UI**: Karena target utama adalah petani, hindari istilah teknis meteorologi di halaman utama. Tampilkan indikator yang lebih ramah, seperti "Kondisi Tanah: Sangat Basah" atau "Suhu Udara: Panas Terik".
- **Notifikasi tepat waktu (push notification)**: Buat notifikasi otomatis saat fase Keuneunong berganti atau saat AI mendeteksi risiko penting.
- **Kamus/ensiklopedia Keuneunong**: Tambahkan menu edukasi tentang fase-fase Keuneunong agar pengguna, termasuk generasi muda, memahami warisan budaya ini.