#include <bits/stdc++.h>
using namespace std;

class DB {
    map<string, string> data;

public:
    bool setkey(string key, string val) {
        if (data.count(key)) {
            return false;
        }
        data[key] = val;
        return true;
    }

    pair<bool, string> getkey(string key) {
        auto it = data.find(key);
        if (it != data.end()) {
            return {true, it->second};
        }
        return {false, ""};
    }

    bool updatekey(string key, string newval) {
        auto it = data.find(key);

        if (it != data.end()) {
            it->second = newval;
            return true;
        }
        return false;
    }

    bool dropkey(string key) {
        auto it = data.find(key);
        if (it != data.end()) {
            data.erase(it);
            return true;
        }
        return false;
    }
};

class Executor {
    private:
        DB& db;

        string handleExit(vector<string>& args) {
            return "EXIT RECV";
        }

        string handleSet(vector<string>& args) {
            if (args.size() != 3) return "ERR MISSING_ARGS";
            if (db.setkey(args[1], args[2])) {
                return "OK";
            } 
            else {
                return "ERR DUPLICATE_KEY";
            }
        }

        string handleGet(vector<string>& args) {
            if (args.size() != 2) return "ERR MISSING_ARGS";
            auto res = db.getkey(args[1]);
            if (!res.first) {
                return "ERR KEY_NOT_FOUND";
            }
            else {
                return res.second;
            }
        }

        string handleUpdate(vector<string>& args) {
            if (args.size() != 3) return "ERR MISSING_ARGS";
            if (db.updatekey(args[1], args[2])) {
                return "OK";
            } else {
                return "ERR KEY_NOT_FOUND";
            }
        }

        string handleDrop(vector<string>& args) {
            if (args.size() != 2) return "ERR MISSING_ARGS";
            if (db.dropkey(args[1])) {
                return "OK";
            } else {
                return "ERR KEY_NOT_FOUND";
            }
        }

        unordered_map<string, function<string(vector<string>&)>> handlers;
    
    public:
        Executor(DB& database) : db(database) {
            handlers["set"] = [this](vector<string>& args) -> string { return handleSet(args); };
            handlers["get"] = [this](vector<string>& args) -> string { return handleGet(args); };
            handlers["update"] = [this](vector<string>& args) -> string { return handleUpdate(args); };
            handlers["drop"] = [this](vector<string>& args) -> string { return handleDrop(args); };
            handlers["exit"] = [this](vector<string>& args) -> string { return handleExit(args); };
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
    DB db;
    cout << "DB created." << endl;
    Executor executor(db);

    while (true) {
        string input;
        getline(cin, input);

        string response = executor.execute(input);

        cout << response << endl;

        if (response == "EXIT RECV") break;
    }

    return 0;
}