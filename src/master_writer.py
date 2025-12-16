import redis
import time

# ===============================================
# --- KONFIGURASI MASTER ---
# ===============================================
MASTER_HOST = '192.168.122.73'
MASTER_PORT = 5000
NUM_KEYS = 1000
KEY_PREFIX = "test_key_"

def run_writer():
    print("## ✍️ Redis Writer: Menulis Data ke Master")
    print(f"Master Target: {MASTER_HOST}:{MASTER_PORT}")
    print("-" * 50)

    try:
        master = redis.Redis(host=MASTER_HOST, port=MASTER_PORT)
        master.ping()
        print("✅ Koneksi ke Master berhasil.")
        
        # --- 1. Cleanup Data Lama ---
        master.flushall()
        print("🧹 Data Master dibersihkan (FLUSHALL).")

        # --- 2. Kirim 1000 write (SET) ke master (DENGAN JEDA PAKSA) ---
        print(f"🔄 Mengirim {NUM_KEYS} write (SET) ke Master...")
        start_write_time = time.time()
        
        # Kirim 500 key pertama
        pipeline = master.pipeline()
        for i in range(500):
            pipeline.set(f"{KEY_PREFIX}{i}", f"value:{i}")
        pipeline.execute()
        
        # Jeda 1 milidetik untuk memastikan lag terdeteksi oleh Replica
        time.sleep(0.001) 
        
        # Kirim 500 key kedua
        pipeline = master.pipeline()
        for i in range(500, NUM_KEYS):
            pipeline.set(f"{KEY_PREFIX}{i}", f"value:{i}")
        pipeline.execute()
        
        end_write_time = time.time()
        print(f"   Selesai menulis total {NUM_KEYS} key dalam {end_write_time - start_write_time:.4f} detik.")

    except redis.exceptions.ConnectionError:
        print(f"❌ GAGAL KONEKSI. Pastikan Redis Master ({MASTER_HOST}:{MASTER_PORT}) berjalan!")
    except Exception as e:
        print(f"❌ Error saat eksekusi writer: {e}")

if __name__ == "__main__":
    run_writer()