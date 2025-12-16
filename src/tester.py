import redis
import time

# ==========================================
# --- KONFIGURASI TOPOLOGI ---
# ==========================================
MASTER_HOST = '192.168.122.73'
MASTER_PORT = 5000

REPLICA_HOST = '192.168.122.192'
REPLICA_PORT = 5001

# --- KONFIGURASI STANDARD (RINGAN) ---
NUM_WRITES = 1000       
# Tidak ada VAL_SIZE besar, kita pakai string biasa

def run_scenario_1():
    print("## 🧪 Skenario 1: Observasi Replication Lag (Payload Ringan)")
    print(f"   Master  : {MASTER_HOST}:{MASTER_PORT}")
    print(f"   Replica : {REPLICA_HOST}:{REPLICA_PORT}")
    print(f"   Jumlah  : {NUM_WRITES} keys (String Pendek)")
    print("-" * 50)

    try:
        # 1. Koneksi
        master = redis.Redis(host=MASTER_HOST, port=MASTER_PORT)
        replica = redis.Redis(host=REPLICA_HOST, port=REPLICA_PORT)
        
        master.ping()
        replica.ping()
        print("✅ Koneksi berhasil.")

        # Bersihkan DB
        master.flushall()

        # ==========================================
        # 2. WRITE PHASE (Payload Ringan)
        # ==========================================
        print(f"🔄 Mengirim {NUM_WRITES} data ke Master...")
        
        pipeline = master.pipeline()
        for i in range(NUM_WRITES):
            # Payload hanya string pendek biasa (misal: "value_0", "value_1")
            pipeline.set(f"key_{i}", f"value_{i}")
            
        start_write_time = time.time()
        pipeline.execute() 
        end_write_time = time.time()
        
        print(f"   Write selesai dalam {end_write_time - start_write_time:.4f} detik.")

        # ==========================================
        # 3. READ PHASE & MEASURE
        # ==========================================
        start_lag_measure = time.time()
        
        print("🔍 Cek Replica...")

        # Cek kondisi awal
        initial_count = replica.dbsize()
        unsynced_keys = NUM_WRITES - initial_count

        print("-" * 50)
        print(f"Total Key Ditulis : {NUM_WRITES}")
        print(f"Total di Replica  : {initial_count}")
        print(f"Key Belum Sinkron : **{unsynced_keys}**")

        if unsynced_keys > 0:
            print("⚠️  Eventual Consistency Terlihat.")
        else:
            print("ℹ️  Data langsung lengkap (Sinkronisasi Instant).")

        # Loop sampai sinkron (Busy Wait)
        while True:
            if replica.dbsize() >= NUM_WRITES:
                break
        
        end_lag_measure = time.time()
        real_lag = end_lag_measure - start_lag_measure

        print("-" * 50)
        print(f"⏱️  REPLICATION LAG: {real_lag:.6f} detik")
        print("-" * 50)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_scenario_1()
