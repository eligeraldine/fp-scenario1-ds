import redis
import time

# ==========================================
# --- KONFIGURASI TOPOLOGI (SESUAI REQUEST) ---
# ==========================================
MASTER_HOST = '192.168.122.73'
MASTER_PORT = 5000

# Kita akan mengukur lag pada Replica-1
REPLICA_HOST = '192.168.122.192'
REPLICA_PORT = 5001

# Replica-2 (Opsional, untuk memastikan cluster sehat)
REPLICA2_HOST = '192.168.122.191'
REPLICA2_PORT = 5002

# --- KONFIGURASI BEBAN KERJA ---
NUM_WRITES = 1000       # Sesuai instruksi Skenario 1
VAL_SIZE = 10240        # 10KB per data (PENTING: Agar lag jaringan terasa)

def run_scenario_1():
    print("## 🧪 Skenario 1: Observasi Replication Lag & Eventual Consistency")
    print(f"   Master  : {MASTER_HOST}:{MASTER_PORT}")
    print(f"   Replica : {REPLICA_HOST}:{REPLICA_PORT}")
    print(f"   Beban   : {NUM_WRITES} keys x {VAL_SIZE/1024} KB")
    print("-" * 50)

    try:
        # 1. Koneksi ke Node
        master = redis.Redis(host=MASTER_HOST, port=MASTER_PORT)
        replica = redis.Redis(host=REPLICA_HOST, port=REPLICA_PORT)
        
        # Cek Koneksi
        master.ping()
        replica.ping()
        print("✅ Koneksi ke Master dan Replica berhasil.")

        # Bersihkan Database agar pengukuran akurat
        master.flushall()
        print("🧹 Database dibersihkan (FLUSHALL).")

        # ==========================================
        # 2. WRITE PHASE (Kirim 1000 Data)
        # ==========================================
        print(f"🔄 Mengirim {NUM_WRITES} write ke Master...")
        
        # Siapkan data dummy 10KB
        payload = "X" * VAL_SIZE
        
        # Gunakan Pipeline agar pengiriman data secepat kilat (membanjiri jaringan)
        pipeline = master.pipeline()
        for i in range(NUM_WRITES):
            pipeline.set(f"key_{i}", payload)
            
        start_write_time = time.time()
        pipeline.execute() # Eksekusi semua perintah sekaligus
        end_write_time = time.time()
        
        print(f"   Selesai menulis ke Master dalam {end_write_time - start_write_time:.4f} detik.")

        # ==========================================
        # 3. READ PHASE (Pengukuran Eventual Consistency)
        # ==========================================
        # Mulai stopwatch pengukuran LAG tepat setelah write selesai
        start_lag_measure = time.time()
        
        print("🔍 Segera membaca dari Replica...")

        # Cek Instan: Apakah data SUDAH ada sekarang juga?
        # Kita pakai dbsize() karena lebih cepat daripada cek 1000 key satu per satu
        initial_count = replica.dbsize()
        unsynced_keys = NUM_WRITES - initial_count

        print("-" * 50)
        print("### Hasil Observasi Konsistensi Eventual ###")
        print(f"Total Key Ditulis Master : {NUM_WRITES}")
        print(f"Total Key di Replica-1   : {initial_count}")
        print(f"Key Belum Tersinkron     : **{unsynced_keys}**")

        if unsynced_keys > 0:
            print("⚠️  EVENTUAL CONSISTENCY TERBUKTI! Ada jeda waktu sinkronisasi.")
        else:
            print("ℹ️  Data langsung lengkap (Jaringan sangat cepat).")

        # ==========================================
        # 4. MEASURE LAG (Looping Sampai Sinkron)
        # ==========================================
        # Loop terus menerus (Busy Wait) tanpa sleep sampai jumlah data sama
        while True:
            current_count = replica.dbsize()
            if current_count >= NUM_WRITES:
                break
            # Tidak ada sleep disini agar akurasi tinggi
        
        end_lag_measure = time.time()
        real_lag = end_lag_measure - start_lag_measure

        print("-" * 50)
        print(f"⏱️  REPLICATION LAG MURNI: {real_lag:.5f} detik")
        print("   (Waktu murni dari Master selesai tulis sampai Replica sinkron)")
        print("-" * 50)

    except redis.exceptions.ConnectionError:
        print("❌ GAGAL KONEKSI. Pastikan IP benar dan Redis berjalan.")
    except Exception as e:
        print(f"❌ Error tidak terduga: {e}")

if __name__ == "__main__":
    run_scenario_1()
