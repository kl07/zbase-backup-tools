/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef HT_HH
#define HT_HH 1

#include <google/sparse_hash_set>

class HashTable {
public:
        bool add(std::string key) {
            google::sparse_hash_set <std::string>::const_iterator it = cache.find(key);
            if (it != cache.end()) {
                return false;
            } else {
                cache.insert(key);
                return true;
            }
        }

private:
    google::sparse_hash_set <std::string> cache;
};

#endif
