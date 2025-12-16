import time
import redis
import sys

def main():
    # Configuration
    MASTER_IP = '192.168.1.10'
    REPLICA_IP = '192.168.1.11'
    PORT = 6379
    NUM_KEYS = 1000
    
    # Connect to Master
    try:
        master = redis.Redis(host=MASTER_IP, port=PORT, decode_responses=True)
        master.ping()
        print(f"Connected to Master at {MASTER_IP}")
    except redis.ConnectionError as e:
        print(f"Error connecting to Master: {e}")
        return

    # Connect to Replica
    try:
        replica = redis.Redis(host=REPLICA_IP, port=PORT, decode_responses=True)
        replica.ping()
        print(f"Connected to Replica at {REPLICA_IP}")
    except redis.ConnectionError as e:
        print(f"Error connecting to Replica: {e}")
        return

    print(f"Starting Consistency Test with {NUM_KEYS} keys...")
    
    start_time = time.time()
    
    # Write to Master using Pipeline for speed
    pipeline = master.pipeline()
    for i in range(1, NUM_KEYS + 1):
        pipeline.set(f"key:{i}", f"value_{i}")
    pipeline.execute()
    
    write_duration = time.time() - start_time
    print(f"Written {NUM_KEYS} keys to Master in {write_duration:.4f} seconds.")

    # Immediately read from Replica
    missed_count = 0
    for i in range(1, NUM_KEYS + 1):
        value = replica.get(f"key:{i}")
        if value is None:
            missed_count += 1
            
    # Calculate results
    replication_lag_percentage = (missed_count / NUM_KEYS) * 100
    
    print("-" * 40)
    print("RESULTS")
    print("-" * 40)
    print(f"Total Keys Written: {NUM_KEYS}")
    print(f"Missed Keys on Replica: {missed_count}")
    print(f"Replication Lag (Missed %): {replication_lag_percentage:.2f}%")
    print("-" * 40)

if __name__ == "__main__":
    main()
