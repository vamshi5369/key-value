#include <iostream>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <fstream>
#include <sstream>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <atomic>

class KVStore {
private:
    std::unordered_map<std::string, std::string> store;
    std::shared_mutex rw_mutex; 
    std::ofstream wal_file;
    std::mutex wal_mutex;
    std::string wal_path;
    std::string snapshot_path;
    
    std::thread snapshot_thread;
    std::atomic<bool> running;

    void log_to_wal(const std::string& op, const std::string& key, const std::string& value = "") {
        std::lock_guard<std::mutex> lock(wal_mutex);
        if (wal_file.is_open()) {
            wal_file << op << " " << key;
            if (!value.empty()) wal_file << " " << value;
            wal_file << "\n";
            wal_file.flush();
        }
    }

    void snapshot_routine() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(30)); // Snapshot every 30 seconds
            
            std::unordered_map<std::string, std::string> store_copy;
            
            // 1. Take a quick read-lock to copy the in-memory state
            {
                std::shared_lock<std::shared_mutex> lock(rw_mutex);
                store_copy = store;
            }

            // 2. Write the copy to the snapshot file
            std::ofstream snap_file(snapshot_path, std::ios::trunc);
            for (const auto& pair : store_copy) {
                snap_file << pair.first << " " << pair.second << "\n";
            }
            snap_file.close();

            // 3. Truncate the WAL safely
            {
                std::lock_guard<std::mutex> lock(wal_mutex);
                wal_file.close();
                wal_file.open(wal_path, std::ios::trunc);
            }
            std::cout << "[System] Snapshot taken. WAL truncated.\n";
        }
    }

public:
    KVStore(const std::string& wal, const std::string& snapshot) 
        : wal_path(wal), snapshot_path(snapshot), running(true) {
        wal_file.open(wal_path, std::ios::app);
        
        // Start the background snapshot thread
        snapshot_thread = std::thread(&KVStore::snapshot_routine, this);
    }

    ~KVStore() {
        running = false;
        if (snapshot_thread.joinable()) {
            snapshot_thread.join();
        }
        if (wal_file.is_open()) wal_file.close();
    }

    std::string get(const std::string& key) {
        std::shared_lock<std::shared_mutex> lock(rw_mutex);
        if (store.find(key) != store.end()) {
            return store.at(key);
        }
        return "ERROR: Key not found";
    }

    std::string set(const std::string& key, const std::string& value) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex);
        store[key] = value;
        log_to_wal("SET", key, value);
        return "OK";
    }

    std::string del(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex);
        if (store.erase(key)) {
            log_to_wal("DELETE", key);
            return "OK";
        }
        return "ERROR: Key not found";
    }
};

void handle_client(int client_socket, KVStore& kv) {
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;

        std::string command(buffer);
        std::istringstream iss(command);
        std::string op, key, value, response;
        iss >> op >> key;

        if (op == "GET") {
            response = kv.get(key);
        } else if (op == "SET") {
            iss >> value;
            response = kv.set(key, value);
        } else if (op == "DELETE") {
            response = kv.del(key);
        } else {
            response = "ERROR: Invalid command";
        }

        response += "\n";
        write(client_socket, response.c_str(), response.length());
    }
    close(client_socket);
}

int main() {
    int server_fd, client_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    KVStore kv("server_wal.log", "snapshot.dat");

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "KV Store listening on port 8080..." << std::endl;

    while (true) {
        client_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (client_socket < 0) {
            perror("accept");
            continue;
        }
        std::thread(handle_client, client_socket, std::ref(kv)).detach();
    }

    return 0;
}