#include <bits/stdc++.h>

using namespace std;
namespace fs = std::filesystem;

class DB {
    map<string, string> data;

    public:
        bool exists(const string& key) {
            if (data.count(key)) return true;
            return false;
        }

        bool setKey(const string& key, const string& val) {
            if (data.count(key)) return false;
            data[key] = val;
            return true;
        }

        pair<bool, string> getKey(const string& key) {
            auto it = data.find(key);
            if (it != data.end()) {
                return {true, it->second};
            }
            return {false, ""};
        }

        bool updateKey(const string& key, const string& newval) {
            auto it = data.find(key);

            if (it != data.end()) {
                it->second = newval;
                return true;
            }
            return false;
        }

        bool dropKey(const string& key) {
            auto it = data.find(key);
            if (it != data.end()) {
                data.erase(it);
                return true;
            }
            return false;
        }

        bool snapshot() {
            string tempfile = "snapshot.db.tmp";
            ofstream outFile(tempfile);

            if (outFile.is_open()) {
                for (const auto& [key, val] : data) {
                    outFile << key << ":" << val << endl;
                }

                outFile.close();
                fs::rename(tempfile, "snapshot.db");
            }
            else return false;

            return true;
        }

        bool loadData() {
            if (!fs::exists("snapshot.db")) return false;
            ifstream inFile("snapshot.db");
            string line;

            if (inFile.is_open()) {
                while (getline(inFile, line)) {
                    size_t colon = line.find(":");
                    if (colon == string::npos) continue;
                    string key = line.substr(0, colon);
                    string val = line.substr(colon + 1);

                    data[key] = val;
                }

                inFile.close();
            }
            else return false;

            return true;
        }
};

class WALManager {
    string logfile;
    
    public:
        bool append(const string& logline) {
            ofstream logFile(logfile, ios::app);
            
            if (logFile.is_open()) {
                logFile << logline << endl;
            }
            else return false;
            return true;
        }
        
        void replay(DB& db) {
            ifstream logFile(logfile);
            string logline;
            
            if (logFile.is_open()) {
                while (getline(logFile, logline)) {
                    stringstream ss(logline);
                    string word;
                    vector<string> args;
                    
                    while (getline(ss, word, '|')) {
                        args.push_back(word);
                    }
                    
                    if (args[0] == "set" && args.size() == 3) {
                        db.setKey(args[1], args[2]);
                    }
                    else if (args[0] == "update" && args.size() == 3) {
                        db.updateKey(args[1], args[2]);
                    }
                    else if (args[0] == "drop" && args.size() == 2) {
                        db.dropKey(args[1]);
                    }
                }
            }
        }

        uintmax_t size() {
            if (!fs::exists(logfile)) return 0;
            return fs::file_size(logfile);
        }

        void truncate() {
            fs::resize_file(logfile, 0);
        }

        WALManager(string filename) : logfile(filename) {
            ofstream touch(filename, ios::app);
        }
};

class Executor {
    private:
        DB& db;
        WALManager& wal;

        string handleExit(vector<string>& args) {
            return "EXIT RECV";
        }

        void checkpoint() {
            db.snapshot();
            wal.truncate();
        }

        string handleSet(vector<string>& args) {
            if (args.size() != 3) return "ERR MISSING_ARGS";
            string logline = "";
            for (string& s : args) {
                logline += s;
                logline += "|";
            }
            logline.pop_back();

            if (db.exists(args[1])) return "ERR DUPLICATE_KEY";
            if (wal.append(logline)) {
                if (!db.setKey(args[1], args[2])) return "ERR DUPLICATE_KEY";
                if (wal.size() > 1024) {
                    checkpoint();
                }
                return "OK";
            }
            else return "ERR UNABLE_TO_WRITE";
        }

        string handleGet(vector<string>& args) {
            if (args.size() != 2) return "ERR INCORRECT_ARGS";
            if (!db.exists(args[1])) return "ERR KEY_NOT_FOUND";

            auto res = db.getKey(args[1]);
            if (!res.first) {
                return "ERR KEY_NOT_FOUND";
            }
            else {
                return res.second;
            }
        }

        string handleUpdate(vector<string>& args) {
            if (args.size() != 3) return "ERR INCORRECT_ARGS";
            string logline = "";
            for (string& s : args) {
                logline += s;
                logline += "|";
            }
            logline.pop_back();

            if (!db.exists(args[1])) return "ERR KEY_NOT_FOUND";
            if (wal.append(logline)) {
                if (!db.updateKey(args[1], args[2])) return "ERR KEY_NOT_FOUND";
                if (wal.size() > 1024) {
                    checkpoint();
                }
                return "OK";
            } else {
                return "ERR UNABLE_TO_WRITE";
            }
        }

        string handleDrop(vector<string>& args) {
            if (args.size() != 2) return "ERR MISSING_ARGS";
            string logline = "";
            for (string& s : args) {
                logline += s;
                logline += "|";
            }
            logline.pop_back();

            if (!db.exists(args[1])) return "ERR KEY_NOT_FOUND";
            if (wal.append(logline)) {
                if (!db.dropKey(args[1])) return "ERR KEY_NOT_FOUND";
                if (wal.size() > 1024) {
                    checkpoint();
                }
                return "OK";
            } else {
                return "ERR UNABLE_TO_WRITE";
            }
        }

        unordered_map<string, function<string(vector<string>&)>> handlers;
    
    public:
        Executor(DB& database, WALManager& WAL) : db(database), wal(WAL){
            handlers.emplace("set", [this](vector<string>& args) { return handleSet(args); });
            handlers.emplace("get", [this](vector<string>& args) { return handleGet(args); });
            handlers.emplace("update", [this](vector<string>& args) { return handleUpdate(args); });
            handlers.emplace("drop", [this](vector<string>& args) { return handleDrop(args); });
            handlers.emplace("exit", [this](vector<string>& args) { return handleExit(args); });
        }

        string execute(string& command) {
            stringstream ss(command);
            string word;
            vector<string> args;

            while (ss >> word) {
                args.push_back(word);
            }

            if (args.size() == 0) return "ERR NOARGS";

            else {
                auto handler = handlers.find(args[0]);
                if (handler == handlers.end()) {
                    return "ERR UNKNOWN_CMD";
                }

                return handler->second(args);
            }
        }
};

int main() {
    cout << "DB created." << endl;
    try {
        DB db;
    
        WALManager wal("wal.log");
        db.loadData();
        wal.replay(db);
    
        Executor executor(db, wal);
    
        while (true) {
            string input;
            getline(cin, input);
    
            string response = executor.execute(input);
    
            cout << response << endl;
    
            if (response == "EXIT RECV") break;
        }
    } catch (const std::exception& e) {
        cerr << "CRASHED WITH ERROR: " << e.what() << endl;
    }

    return 0;
}