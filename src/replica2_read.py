import redis
import time

# ===============================================
# --- KONFIGURASI REPLICA 2 ---
# ===============================================
MASTER_HOST = '192.168.122.73'
REPLICA_HOST = '192.168.122.191'
REPLICA_PORT = 5002
REPLICA_NAME = 'Replica-2'

NUM_KEYS = 1000
KEY_PREFIX = "test_key_"

def test_replica_lag():
    """Menguji konsistensi dan lag untuk Replica 2."""
    
    print(f"## 🧪 Skenario 1 Tester: Mengamati Lag & Konsistensi ({REPLICA_NAME})")
    print(f"Master: {MASTER_HOST}, Target: {REPLICA_HOST}:{REPLICA_PORT}")
    print("--------------------------------------------------")
    
    try:
        replica_conn = redis.Redis(host=REPLICA_HOST, port=REPLICA_PORT, decode_responses=True)
        replica_conn.ping()
        print("✅ Koneksi ke Replica 2 berhasil.")
        
        # --- 1. Initial Consistency Check ---
        unsynced_keys = 0
        pipe = replica_conn.pipeline()
        for i in range(NUM_KEYS):
            pipe.get(f"{KEY_PREFIX}{i}")
        results = pipe.execute()
        
        for result in results:
            if result is None:
                unsynced_keys += 1
            
        print("--------------------------------------------------")
        print("### Hasil Observasi Konsistensi Eventual (Awal) ###")
        print(f"Key yang Belum Tersinkron di {REPLICA_NAME}: {unsynced_keys}")
        
        # --- 2. Mengukur Replication Lag ---
        
        start_lag_measure = time.time()
        current_unsynced = unsynced_keys
        
        while current_unsynced > 0:
            current_unsynced = 0
            pipe = replica_conn.pipeline()
            for i in range(NUM_KEYS):
                pipe.get(f"{KEY_PREFIX}{i}")
            
            results = pipe.execute()
            
            for result in results:
                if result is None:
                    current_unsynced += 1
                    
            if current_unsynced > 0:
                time.sleep(0.0001) 
        
        end_lag_measure = time.time()
        lag_time = end_lag_measure - start_lag_measure
        
        print(f"\n**Replikasi selesai sepenuhnya dalam {lag_time:.4f} detik (Replication Lag).**")
        print(f"✅ STATUS: SUKSES. Semua {NUM_KEYS} data berhasil direplikasi ke {REPLICA_NAME}.")
        
    except redis.exceptions.ConnectionError:
        print(f"\n❌ GAGAL terhubung ke {REPLICA_NAME} ({REPLICA_HOST}:{REPLICA_PORT}). Pastikan server berjalan.")
    except Exception as e:
        print(f"❌ Error saat eksekusi tester: {e}")
    finally:
        print("==================================================")


if __name__ == '__main__':
    test_replica_lag()