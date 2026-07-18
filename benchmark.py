import socket
import threading
import time
import random

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8080
NUM_CLIENTS = 100
OPERATIONS_PER_CLIENT = 1000

def client_worker(client_id, results):
    """Simulates a single client sending SET and GET requests."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_HOST, SERVER_PORT))
        
        start_time = time.time()
        for i in range(OPERATIONS_PER_CLIENT):
            key = f"user_{client_id}_{i % 100}"
            value = f"data_{random.randint(1, 1000)}"
            
            # 50% chance of SET or GET
            if random.random() > 0.5:
                command = f"SET {key} {value}"
            else:
                command = f"GET {key}"
                
            s.sendall(command.encode('utf-8'))
            response = s.recv(1024)
            
        end_time = time.time()
        s.close()
        
        # Record total time taken by this thread
        results[client_id] = end_time - start_time
    except Exception as e:
        print(f"Client {client_id} error: {e}")
        results[client_id] = 0

def run_benchmark():
    print(f"Starting benchmark with {NUM_CLIENTS} concurrent clients...")
    print(f"Each client will perform {OPERATIONS_PER_CLIENT} operations.")
    
    threads = []
    results = [0] * NUM_CLIENTS
    global_start_time = time.time()
    
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=client_worker, args=(i, results))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    global_end_time = time.time()
    total_time = global_end_time - global_start_time
    total_ops = NUM_CLIENTS * OPERATIONS_PER_CLIENT
    throughput = total_ops / total_time
    
    print("\n--- Benchmark Results ---")
    print(f"Total Operations: {total_ops}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} ops/sec")
    print(f"Average Latency: {(total_time / total_ops) * 1000:.2f} ms")

if __name__ == "__main__":
    run_benchmark()